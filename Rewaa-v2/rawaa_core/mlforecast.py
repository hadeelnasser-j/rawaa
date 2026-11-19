"""Machine-learning forecaster: one global Random Forest trained on the trusted NDVI series of ALL parks.

Why global: a single park has ~95 monthly readings, too few for a learning model on its own. Pooling every
park gives several hundred (history -> next reading) examples, so the model can learn patterns that repeat
across Jeddah's parks (seasonal swings, recovery after a dip, damping of spikes).

Leakage guard: a model used to forecast a park from readings up to date C is trained only on examples whose
target reading is dated on or before C, from any park. The backtest therefore stays honest: the model never
sees the reading it is asked to predict, nor anything after it.

Cost guard: training happens in a background thread and models are cached per (data signature, cutoff date).
Until the cache is warm, `fitter()` returns None and the platform simply uses the statistical models.
"""
from __future__ import annotations
import hashlib
import logging
import math
import statistics
import threading
from datetime import date, timedelta

log = logging.getLogger('rawaa.ml')

N_TREES = 100
MIN_LEAF = 8
MIN_SAMPLES = 60
STEP_DAYS = 30
MAX_ORIGINS = 8  # must match forecast.backtest

_LOCK = threading.Lock()
_STATE = {'sig': None, 'models': {}, 'ready': False, 'busy': False, 'samples': 0, 'error': None}


def _nearest(pts, d, tol=20):
    best = None
    for pd_, v in pts:
        dd = abs((pd_ - d).days)
        if dd <= tol and (best is None or dd < best[0]):
            best = (dd, v)
    return None if best is None else best[1]


def features(hist, target: date):
    """Features describing the history before `target`. The model predicts the change from the last reading."""
    y = [v for _, v in hist]
    y1 = y[-1]
    y2 = y[-2] if len(y) > 1 else y1
    y3 = y[-3] if len(y) > 2 else y2
    m6 = statistics.mean(y[-6:])
    m12 = statistics.mean(y[-12:])
    ly = _nearest(hist, target - timedelta(days=365))
    ly_prev = _nearest(hist, hist[-1][0] - timedelta(days=365))
    has = ly is not None and ly_prev is not None
    seas = (ly - ly_prev) if has else 0.0
    gap = (target - hist[-1][0]).days
    m = target.month
    return [y1, y1 - y2, y2 - y3, m6 - y1, m12 - y1, seas, 1.0 if has else 0.0,
            math.sin(2 * math.pi * m / 12), math.cos(2 * math.pi * m / 12), gap / 30.0]


FEATURE_NAMES = ['last NDVI', 'last change', 'previous change', '6-month mean - last', '12-month mean - last',
                 'same-season change last year', 'has last-year season', 'month (sin)', 'month (cos)', 'gap (months)']


def _samples(pool, cutoff: date):
    X, Y = [], []
    for pts in pool:
        for i in range(3, len(pts)):
            if pts[i][0] > cutoff:
                break
            X.append(features(pts[:i], pts[i][0]))
            Y.append(pts[i][1] - pts[i - 1][1])
    return X, Y


def signature(pool) -> str:
    h = hashlib.sha1()
    for pts in pool:
        for d, v in pts:
            h.update(f'{d.isoformat()}:{v:.5f};'.encode())
        h.update(b'|')
    return h.hexdigest()


def _train(pool, cutoff: date):
    import numpy as np
    from sklearn.ensemble import RandomForestRegressor
    X, Y = _samples(pool, cutoff)
    if len(X) < MIN_SAMPLES:
        return None, len(X)
    mdl = RandomForestRegressor(n_estimators=N_TREES, min_samples_leaf=MIN_LEAF, max_features=0.6,
                                random_state=0, n_jobs=1)
    mdl.fit(np.array(X), np.array(Y))
    return mdl, len(X)


def _needed_cutoffs(pool):
    cut = set()
    for pts in pool:
        n = len(pts)
        for i in range(max(4, n - MAX_ORIGINS), n):
            cut.add(pts[i - 1][0])
        if pts:
            cut.add(pts[-1][0])
    return sorted(cut)


def _warm(pool, sig):
    try:
        models, samples = {}, 0
        for c in _needed_cutoffs(pool):
            mdl, n = _train(pool, c)
            models[c] = mdl
            samples = max(samples, n)
        with _LOCK:
            if _STATE['sig'] == sig:
                _STATE.update(models=models, ready=True, busy=False, samples=samples, error=None)
        log.info('ML forecaster ready: %d models, up to %d training examples', len(models), samples)
    except Exception as ex:  # never break the platform because of the ML layer
        log.exception('ML forecaster warm-up failed')
        with _LOCK:
            if _STATE['sig'] == sig:
                _STATE.update(busy=False, ready=False, error=str(ex)[:200])


def prepare(pool, background: bool = True) -> bool:
    """Make sure models exist for this pool. Returns True if ready now."""
    pool = [p for p in pool if len(p) >= 4]
    if not pool:
        return False
    sig = signature(pool)
    with _LOCK:
        if _STATE['sig'] == sig and (_STATE['ready'] or _STATE['busy']):
            return _STATE['ready']
        _STATE.update(sig=sig, models={}, ready=False, busy=True, error=None)
    if background:
        threading.Thread(target=_warm, args=(pool, sig), daemon=True, name='rawaa-ml-warm').start()
        return False
    _warm(pool, sig)
    return _STATE['ready']


def status():
    with _LOCK:
        return {'ready': _STATE['ready'], 'busy': _STATE['busy'], 'models': len(_STATE['models']),
                'training_examples': _STATE['samples'], 'error': _STATE['error']}


def fitter():
    """Return fit(hist) for forecast.py, or None while the models are not ready."""
    with _LOCK:
        if not _STATE['ready']:
            return None
        models = _STATE['models']

    def fit(hist):
        if len(hist) < 4:
            return None
        mdl = models.get(hist[-1][0])
        if mdl is None:
            return None
        import numpy as np
        y1 = hist[-1][1]
        cache = {}

        def pred(target):
            if target not in cache:
                cache[target] = y1 + float(mdl.predict(np.array([features(hist, target)]))[0])
            return cache[target]

        def f(target):
            gap = (target - hist[-1][0]).days
            if gap < 20:  # shorter than one revisit step: interpolate towards the next monthly value
                nxt = pred(hist[-1][0] + timedelta(days=STEP_DAYS))
                return y1 + (nxt - y1) * max(gap, 0) / STEP_DAYS
            return pred(target)
        return f
    return fit
