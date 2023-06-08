def heuristic(demands):
    n = []
    r = []
    T = 0
    B = 0
    for t in range(demands):
        while True:
            u = 0
            for i in range(t-T+1, t+1):
                if demands[i] > r[i]:
                    u += 1
            if u >= B:
                n[t] += 1
                for i in range(t, (t + T - 1) + 1):
                    r[i] += 1
                for i in range(t - T + 1, (t - 1) + 1):
                    r[i] += 1
            else: break
    return (n, r)

# t2.nano
# on demand: 0.0058
# reserved: 1 year, upfront = 15, monthly = 1.24 (R = 29.88)