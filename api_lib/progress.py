from time import perf_counter as clock
from time import sleep
import multiprocessing as mp
import _thread as thread

class Progress:

    __slots__ = (
        'text', '__lapse', '__total', '__lock', '__done', '_done',
        '__start_time', '__last_length', '__finished',
    )

    STEPS = 20

    def __init__(self, total: int, lapse: float=None, text: str='Progresso: '):
        ## Some text
        self.text = text

        ## Update Lapse
        self.__lapse = 0.5 if lapse is None else lapse

        ## Total steps
        self.__total = total

        ## Lock for progress track
        self.__lock = mp.Lock()
        self.__done = mp.Value('i', 0)

        self.__start_time = clock()

        ## Previous output string lenght
        self.__last_length = 0

        ## Finished
        self.__finished = False
        
        print(self, end='\r')

    @property
    def lock(self):
        return self.__lock

    @property
    def total(self):
        return self.__total

    def start(self):
        self.__start_time = clock()

    @property
    def done(self):
        with self.lock: return self.__done.value

    @property
    def start_time(self):
        return self.__start_time
    
    @property
    def total_time(self):
        return clock() - self.start_time

    @property
    def finished(self):
        return self.__finished or (self.done >= self.total)

    def __next__(self):
        if not self.finished:
            with self.lock: self.__done.value += 1
        else:
            raise StopIteration

    def display(self):
        self.start()
        while not self.finished:
            self.update()
            sleep(self.__lapse)
        else:
            self.update()
            print(f'Time elapsed: {self.total_time:.1f}s')

    def finish(self):
        self.__finished = True
            
    def track(self, lapse:float=None) -> int:
        if lapse is not None: 
            self.__lapse = lapse
        return thread.start_new(self.display, ())

    @property
    def end(self):
        return '\n' if self.finished else '\r'

    def update(self):
        print(self, self.padding, end=self.end)
        self.__last_length = self.length
        
    def __str__(self):
        """ output string;
        """
        return f'{self.text} {self.bar} {self.done}/{self.total} {100 * self.ratio:2.2f}% eta: {self.eta} rate: {self.rate:.2f}/s'

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
        return self.done / self.total

    @property
    def rate(self):
        """ steps per second;
        """
        return self.done / self.total_time
    
    @property
    def eta(self) -> str:
        if not self.done:
            return "?"
        s = (self.total_time / self.done) * (self.total - self.done)
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