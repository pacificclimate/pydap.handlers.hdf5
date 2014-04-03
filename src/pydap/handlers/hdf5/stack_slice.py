class StackableSlice(object):
    def __init__(self, *args):
        for arg in args:
            assert type(arg) == int or arg == None

        self.slice = slice(*args)

    def __repr__(self):
        return 'StackableSlice({}, {}, {})'.format(self.start, self.stop, self.step)

    def __str__(self):
        return 'StackableSlice({}:{}:{})'.format(self.start, self.stop, self.step)

    def __add__(self, other):
        '''other is either a StackableSlice or a slice'''
        if type(other) == StackableSlice:
            other = other.slice
        if self.slice == slice(None):
            return StackableSlice(other.start, other.stop, other.step)
        if other == slice(None):
            return StackableSlice(self.slice.start, self.slice.stop, self.slice.step)

        start_sum = self.start + other.start
        if self.step:
            self_range = range(self.start, self.stop, self.step)
        else:
            self_range = range(self.start, self.stop)
        num_skips = len(range(self.start, max(self_range)+1)) - len(self_range)
        new_stop = other.stop + self.start + num_skips 

        # FIXME: we should be able to keep consistency some other way here
        if self.step and not other.step:
            new_step = self.step
        elif other.step and not self.step:
            new_step = other.step
        elif not (self.step and other.step):
            new_step = 1
        else:
            new_step = self.step * other.step
        
        return StackableSlice(start_sum, new_stop, new_step)

    def __getitem__(self, slice_):
        if type(slice_) == int:
            slice_ = slice(slice_, slice_ + 1, 1)
        return self.__add__(slice_)

    def __eq__(self, other):
        return True if self.start == other.start and self.stop == other.stop and self.step == other.step else False
        

    @property
    def start(self):
        return self.slice.start
    @property
    def stop(self):
        return self.slice.stop
    @property
    def step(self):
        return self.slice.step
