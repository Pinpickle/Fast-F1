import copy
import json
from typing import Literal, Union
import warnings

from fastf1.api import Cache
from fastf1.utils import recursive_dict_get
from fastf1.version import __version__

import pandas as pd

base_url = 'https://ergast.com/api/f1'
_headers = {'User-Agent': f'FastF1/{__version__}'}


class _ErgastResponseItem:
    def __init__(self, response):
        self._response = response

    def __repr__(self):
        return "<meta>"


class ErgastResultFrame(pd.DataFrame):
    # TODO: naming convention camelCase (orignal) or UpperCamelCase (FastF1)

    _internal_names = ['base_class_view']

    def __init__(self, data, *args, response=None, **kwargs):
        if data is None:
            data = self._prepare_response(response)
        super().__init__(data, **kwargs)

    @staticmethod
    def _prepare_response(response):
        data = copy.deepcopy(response)  # TODO: efficiency?
        for i in range(len(data)):
            if drv := data[i].pop('Driver', None):
                data[i].update(
                    {'Driver': drv.get('code', ""),
                     'DriverNumber': drv.get('permanentNumber', "")}
                )
            if constr := data[i].pop('Constructors', None):
                data[i].update(
                    {'Constructors': [e.get('name', "") for e in constr]}
                )
            if loc := data[i].pop('Location', None):
                data[i].update(
                    {'Lat': float(loc.get('lat', 0)),
                     'Long': float(loc.get('long', 0)),
                     'Locality': loc.get('locality', ""),
                     'Country': loc.get('country', "")}
                )

            data[i]['__ErgastResponse'] = _ErgastResponseItem(response[i])

        return data

    def __repr__(self):
        view_cols = list(self.columns)
        if '__ErgastResponse' in view_cols:
            view_cols.remove('__ErgastResponse')
        return pd.DataFrame(self)[view_cols].__repr__()

    @property
    def _constructor(self):
        def _new(*args, **kwargs):
            return ErgastResultFrame(*args, **kwargs).__finalize__(self)

        return _new

    @property
    def _constructor_sliced(self):
        def _new(*args, **kwargs):
            name = kwargs.get('name')
            if name and name in self.columns:
                # vertical slice
                return pd.Series(*args, **kwargs).__finalize__(self)

            # horizontal slice
            return ErgastResultSeries(*args, **kwargs).__finalize__(self)

        return _new

    @property
    def base_class_view(self):
        """For a nicer debugging experience; can view DataFrame through
        this property in various IDEs"""
        return pd.DataFrame(self)

    def get_ergast_response(self):
        if '__ErgastResponse' in self:
            return [getattr(e, '_response') for e in self['__ErgastResponse']]


class ErgastResultSeries(pd.Series):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def _constructor(self):
        def _new(*args, **kwargs):
            return ErgastResultSeries(*args, **kwargs).__finalize__(self)

        return _new

    def __repr__(self):
        view_idx = list(self.index)
        if '__ErgastResponse' in view_idx:
            view_idx.remove('__ErgastResponse')
        return pd.Series(self)[view_idx].__repr__()

    def get_ergast_response(self):
        if '__ErgastResponse' in self:
            return getattr(self['__ErgastResponse'], '_response')
        return None


class ErgastSelectionObject:
    # TODO: maximum size of response and offset relevant?

    def __init__(self, selector: str):
        self._selector = selector

    def _get(self, url: str) -> Union[dict, list]:
        r = Cache.requests_get(url, headers=_headers)
        if r.status_code == 200:
            try:
                return json.loads(r.content.decode('utf-8'))
            except Exception as exc:
                raise ErgastJsonException(
                    f"Failed to parse Ergast response ({url})"
                ) from exc
        else:
            raise ErgastInvalidRequest(
                f"Invalid request to Ergast ({url})"
            )

    def get_circuits(self) -> ErgastResultFrame:
        url = f"{base_url}{self._selector}/circuits.json"
        resp = self._get(url)['MRData']['CircuitTable']['Circuits']
        return ErgastResultFrame(None, response=resp)

    def get_driver_standings(self) -> ErgastResultFrame:
        url = f"{base_url}{self._selector}/driverStandings.json"
        resp = self._get(url)['MRData']['StandingsTable']['StandingsLists']\
            [0]['DriverStandings']
        return ErgastResultFrame(None, response=resp)

    def get_constructor_standings(self) -> ErgastResultFrame:
        url = f"{base_url}{self._selector}/constructorStandings.json"
        resp = self._get(url)['MRData']['StandingsTable']['StandingsLists']\
            [0]['ConstructorStandings']
        return ErgastResultFrame(None, response=resp)


class Ergast(ErgastSelectionObject):
    def __init__(self):
        super().__init__(selector="")

    def select(self,
               season: Union[Literal['current'], int] = None,
               round: Union[Literal['last'], int] = None,
               circuit: str = None,
               constructor: str = None,
               driver: str = None,
               # TODO grid=position,
               # TODO results=position,
               # TODO fastest=rank,
               # TODO status=statusId
               ) -> ErgastSelectionObject:

        selector = ""

        if season is not None:
            selector += f"/{season}"
        if round is not None:
            selector += f"/{round}"
        if circuit is not None:
            selector += f"/circuits/{circuit}"
        if constructor is not None:
            selector += f"/constructors/{constructor}"
        if driver is not None:
            selector += f"/drivers/{driver}"

        return ErgastSelectionObject(selector)


def fetch_results(year, gp, session):
    """session can be 'Qualifying' or 'Race'
    mainly to port on upper level libraries
    """
    if session == 'Race':
        day = 'results'
        sel = 'Results'
    elif session == 'Qualifying':
        day = 'qualifying'
        sel = 'QualifyingResults'
    elif session in ('Sprint Qualifying', 'Sprint'):
        day = 'sprint'
        sel = 'SprintResults'

    return _parse_ergast(fetch_day(year, gp, day))[0][sel]


def fetch_season(year):
    url = f"{base_url}/{year}.json"
    return _parse_ergast(_parse_json_response(
        Cache.requests_get(url, headers=_headers))
    )


def fetch_weekend(year, gp):
    warnings.warn(
        "`fetch_weekend()` is deprecated and will be"
        "removed without a direct replacement in a "
        "future version.",
        FutureWarning
    )
    url = f"{base_url}/{year}/{gp}.json"
    data = _parse_ergast(_parse_json_response(
        Cache.requests_get(url, headers=_headers)
    ))[0]
    url = ("https://www.mapcoordinates.net/admin/component/edit/"
           + "Vpc_MapCoordinates_Advanced_GoogleMapCoords_Component/"
           + "Component/json-get-elevation")
    loc = data['Circuit']['Location']
    body = {'longitude': loc['long'], 'latitude': loc['lat']}
    res = _parse_json_response(Cache.requests_post(url, data=body))
    data['Circuit']['Location']['alt'] = res['elevation']
    return data


def fetch_day(year, gp, day):
    """day can be 'qualifying' or 'results'
    """
    url = f"{base_url}/{year}/{gp}/{day}.json"
    return _parse_json_response(Cache.requests_get(url, headers=_headers))


def _parse_json_response(r):
    if r.status_code == 200:
        return json.loads(r.content.decode('utf-8'))
    else:
        warnings.warn(f"Request returned: {r.status_code}")
        return None


def _parse_ergast(data):
    return data['MRData']['RaceTable']['Races']


class ErgastException(Exception):
    pass


class ErgastJsonException(ErgastException):
    pass


class ErgastInvalidRequest(ErgastException):
    pass
