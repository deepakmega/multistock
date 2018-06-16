import numpy as np
import talib
close = [10604.8, 10603.2, 10602.5, 10603.95, 10602.65, 10602.95, 10603.95]
a = np.asarray(close)

real = talib.RSI(a, timeperiod=5)
print(real)