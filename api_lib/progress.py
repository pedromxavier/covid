from time import perf_counter as clock
import multiprocessing as mp
import threading

class Progress:

    __slots__ = (
        'text', '__lapse', '__total', '__start', '__lock', '__done', '_done',
        '__start_time', '__last_length', '__uptodate',
    )

    STEPS = 20

    def __init__(self, total: int, start: int=0, lapse: float=None, text: str='Progresso: '):
        ## Some text
        self.text = text

        ## Update Lapse
        self.__lapse = 0.5 if lapse is None else lapse

        ## Total steps
        self.__total = total
        self.__start = start

        ## Lock for progress track
        self.__lock = mp.Lock()
        self.__done = mp.Value('i', 0)
        self._done = self.__done.value

        self.__start_time = clock()

        ## Previous output string lenght
        self.__last_length = 0

        ## Track need for update in done
        self.__uptodate = False
        
        print(self, end='\r')

    @property
    def lock(self):
        return self.__lock

    @property
    def total(self):
        return self.__total

    @property
    def start(self):
        return self.__start

    @property
    def done(self):
        if not self.__uptodate:
            with self.lock:
                self._done = self.__done.value
            self.__uptodate = True
        return self._done

    @property
    def start_time(self):
        return self.__start_time
    
    @property
    def total_time(self):
        return clock() - self.start_time

    @property
    def finished(self):
        return (self.start + self.done) >= self.total

    def __next__(self):
        if not self.finished:
            with self.lock: self.__done.value += 1
            self.__uptodate = False
        else:
            raise StopIteration

    def display(self):
        while not self.finished:
            print(self, self.padding, end='\r')
            self.__last_length = self.length
        else:
            print(self, self.padding, end='\n')
            print(f'Time elapsed: {self.total_time}')
            
    def track(self) -> threading.Thread:
        thread = threading.Thread(target=self.display, args=())
        thread.start()
        return thread
        
    def __str__(self):
        """ output string;
        """
        return f'{self.text} {self.bar} {self.start + self.done}/{self.total} {100 * self.ratio:2.2f}% eta: {self.eta} rate: {self.rate:.2f}/s'

    @property
    def padding(self):
        """ padding needed to erase previous output;
        """
        return " " * (self.__last_length - self.length)

    @property
    def length(self):
        """ output string lenght;
        """
        return len(str(self))

    @property
    def ratio(self) -> float:
        """ progress ratio; value in [0, 1]
        """
        return (self.start + self.done) / self.total

    @property
    def rate(self):
        """ steps per second;
        """
        return self.done / self.total_time
    
    @property
    def eta(self) -> str:
        if not self.done:
            return "?"
        s = (self.total_time / self.done) * (self.total - (self.start + self.done))
        if s >= 60:
            m, s = divmod(s, 60)
            if m >= 60:
                h, m = divmod(m, 60)
                if h >= 24:
                    d, h = divmod(h, 24)
                    return f"{int(d):d}d{int(h):d}h{int(m):d}m{int(s):d}s"
                else:
                    return f"{int(h):d}h{int(m):d}m{int(s):d}s"
            else:
                return f"{int(m):d}m{int(s):d}s"
        else:
            return f"{int(s):d}s"

    @property
    def bar(self) -> str:
        if self.ratio == 0.0:
            return f"[{' ' * self.STEPS}]"
        elif self.ratio < 1:
            return f"[{int(self.ratio * self.STEPS) * '='}>{int((1 - self.ratio) * self.STEPS) * ' '}]"
        else:
            return f"[{'=' * self.STEPS}]"