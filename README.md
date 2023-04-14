# Footprints
proof of concept for plotly and tick data

[mplfinance Issue # 572 (enhancement)] (https://github.com/matplotlib/mplfinance/issues/572)

#### No need to use numba

Revised code which does not use numba runs faster than numba @njit code (former code).

```ruby
with @njit: 16.120 seconds
without @njit: 15.451 seconds
```

