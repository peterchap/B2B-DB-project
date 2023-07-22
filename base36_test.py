import sys
import base_repr 
'''
class Base36(BaseRepr):
    def __init__(self, padding: int = 0, byteorder: str = sys.byteorder, encoding: str = 'utf-8'):
        super().__init__(36, padding, byteorder, encoding)

base = Base36(padding=0, byteorder='big', encoding='utf-8')

print(base.repr_to_int('pchaplin@esbconnect.com'))
'''
print(base_repr.str_to_repr('pchaplin@esbconnect.com', base=62, byteorder='little', encoding='utf-8'))