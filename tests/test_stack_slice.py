import pytest

from stack_slice import StackableSlice

ss = StackableSlice
@pytest.mark.parametrize(('slice_zero','slice_one','expected_output'), [
    (ss(None), ss(None), range(10)),
    (ss(None), ss(2, 8, 1), range(2, 8)),
    (ss(2, 8, 1), ss(0, 6, 2), [2, 4, 6]),
    (ss(2, 8, 2), ss(0, 3, 1), [2, 4, 6]),
    (ss(2, 8, 2), ss(0, 4, 2), [2, 6]),
])
def test_foo(slice_zero, slice_one, expected_output):
    array = range(10)
    slice_result = slice_zero + slice_one
    assert array[slice_result.slice] == expected_output

def test_slicable():
    x = ss(1, 9, 1)
    assert x[2:8:2] == ss(3, 9, 2)

def test_getitem():
    x = ss(1, 9, 1)
    x[1] == ss(1, 2, 1)

@pytest.mark.parametrize(('slice_zero', 'slice_one', 'expected_output'), [
    (ss(None), slice(None), True),
    (ss(1, 9, 1), slice(1, 9, 1), True),
    (ss(0, 5), slice(0, 5), True)
])
def test_eq(slice_zero, slice_one, expected_output):
    if expected_output:  
        assert slice_zero == slice_one
    else:
        assert slice_zero != slice_one
