from setup import datetime
import re
from setup import timedelta

'''This module is to hold the Filemask class, used to adjust expected
filemask configurations to "actual" filemasks that could have been
delivered by vendors.'''

class Filemask:
    '''"Expected" filemask object, which includes both the generic mask
    and the mask for the preceding date, as expected by the PTCCService.'''
    def __init__(
        self,
        mask,
        offset
    ):
        self.now = self._offset(offset)
        self.mask: str = self._fix(str(mask))
    
    def _offset(self, offset: int) -> timedelta:
        '''Offset the filemask based on the Offset configuration value.'''
        return datetime.now() - timedelta(days=offset)

    def _get_date_masks(self) -> dict:
        '''Dictionary of expected filename components.'''
        dates = {
            'yyyy': self.now.strftime('%Y'),
            'yy': self.now.strftime('%y'),
            'mm': self.now.strftime('%m'),
            'dd': self.now.strftime('%d')
        }
        return dates

    def _fix(self, mask) -> str:
        '''Private function callable when building DataFrame (in df.py)
        to fix filename masks.'''
        masks = self._get_date_masks()
        for i in masks:
            if re.search(i, mask) is not None:
                mask = re.sub(i, str(masks[i]), mask)
        self.mask = re.sub(r'\{\}]', '', mask)
        return self.mask