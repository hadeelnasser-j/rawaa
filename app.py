"""Rawaa MVP — bilingual green-space operations and optional Sentinel-2 statistics.

Demo observations are explicitly labeled and never silently promoted to live readings.
The app is a local MVP, not production-certified or a validated ML forecasting service.
"""
from __future__ import annotations
import base64
import binascii
import csv
import hashlib
import hmac
import io
import json
import re
import math
import os
import secrets
import sqlite3
import statistics
import threading
import time
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib import parse, request, error

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

ROOT = Path(__file__).resolve().parent
DB_PATH = Path(os.environ.get('RAWAA_DB') or ROOT / 'data' / 'rawaa.sqlite3')
DB_PATH.parent.mkdir(parents=True, exist_ok=True)
app = FastAPI(title='Rawaa | رَوَاء API', version='0.1.0')
app.mount('/static', StaticFiles(directory=str(ROOT / 'static')), name='static')
DB_LOCK = threading.RLock()


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec='seconds')

@contextmanager
def connect():
    with DB_LOCK:
        db = sqlite3.connect(DB_PATH, timeout=15)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA foreign_keys=ON')
        try:
            yield db
            db.commit()
        finally:
            db.close()


def asdict(row): return dict(row) if row is not None else None

def password_hash(password, salt=None):
    salt = salt or secrets.token_bytes(16)
    dk = hashlib.pbkdf2_hmac('sha256', password.encode(), salt, 260_000)
    return salt.hex()+':'+dk.hex()

def password_ok(password, encoded):
    try:
        s, expected = encoded.split(':')
        actual = password_hash(password, bytes.fromhex(s)).split(':')[1]
        return hmac.compare_digest(actual, expected)
    except Exception:
        return False


def square(lng, lat, d=0.002):
    return {'type':'Polygon','coordinates':[[
        [lng-d,lat-d*.68],[lng+d,lat-d*.68],[lng+d,lat+d*.68],
        [lng-d,lat+d*.68],[lng-d,lat-d*.68]
    ]]}


def init_db():
    with connect() as db:
        db.executescript('''
            CREATE TABLE IF NOT EXISTS users(
              id INTEGER PRIMARY KEY, email TEXT UNIQUE NOT NULL, display TEXT NOT NULL,
              role TEXT NOT NULL CHECK(role IN ('regulator','operator')),
              pw_hash TEXT NOT NULL, created_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS sessions(
              id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, token_hash TEXT UNIQUE NOT NULL,
              expires INTEGER NOT NULL, FOREIGN KEY(user_id) REFERENCES users(id));
            CREATE TABLE IF NOT EXISTS areas(
              id INTEGER PRIMARY KEY, name_ar TEXT NOT NULL, name_en TEXT NOT NULL,
              category TEXT NOT NULL DEFAULT 'Park', assigned_operator INTEGER,
              geometry TEXT NOT NULL, irrigation TEXT DEFAULT '', plants TEXT DEFAULT '',
              planting_date TEXT, demo INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL,
              FOREIGN KEY(assigned_operator) REFERENCES users(id));
            CREATE TABLE IF NOT EXISTS observations(
              id INTEGER PRIMARY KEY, area_id INTEGER NOT NULL, measured_at TEXT NOT NULL,
              ndvi REAL, ndmi REAL, valid_pixels_pct REAL NOT NULL, source TEXT NOT NULL,
              is_demo INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL,
              UNIQUE(area_id, measured_at, source), FOREIGN KEY(area_id) REFERENCES areas(id));
            CREATE TABLE IF NOT EXISTS alerts(
              id INTEGER PRIMARY KEY, area_id INTEGER NOT NULL, severity TEXT NOT NULL,
              reason_ar TEXT NOT NULL, reason_en TEXT NOT NULL,
              status TEXT NOT NULL DEFAULT 'new', created_at TEXT NOT NULL,
              FOREIGN KEY(area_id) REFERENCES areas(id));
            CREATE TABLE IF NOT EXISTS tasks(
              id INTEGER PRIMARY KEY, area_id INTEGER NOT NULL, alert_id INTEGER,
              title_ar TEXT NOT NULL, title_en TEXT NOT NULL, assigned_to INTEGER NOT NULL,
              status TEXT NOT NULL DEFAULT 'open', priority TEXT NOT NULL DEFAULT 'medium',
              due_at TEXT, action TEXT DEFAULT '', actual_cause TEXT DEFAULT '',
              notes TEXT DEFAULT '', created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
              FOREIGN KEY(area_id) REFERENCES areas(id),
              FOREIGN KEY(alert_id) REFERENCES alerts(id),
              FOREIGN KEY(assigned_to) REFERENCES users(id));
            CREATE TABLE IF NOT EXISTS task_photos(
              task_id INTEGER NOT NULL, kind TEXT NOT NULL CHECK(kind IN ('before','after')),
              filename TEXT NOT NULL, mime TEXT NOT NULL, uploaded_by INTEGER NOT NULL, created_at TEXT NOT NULL,
              PRIMARY KEY(task_id,kind), FOREIGN KEY(task_id) REFERENCES tasks(id));
            CREATE TABLE IF NOT EXISTS activity(
              id INTEGER PRIMARY KEY, user_id INTEGER, verb TEXT NOT NULL, target TEXT NOT NULL,
              details TEXT DEFAULT '', created_at TEXT NOT NULL);
        ''')
        if db.execute('SELECT count(*) FROM users').fetchone()[0]: return
        db.execute('INSERT INTO users(email,display,role,pw_hash,created_at) VALUES (?,?,?,?,?)',
                   ('regulator@rawaa.local','Jeddah Municipality — Demo','regulator',password_hash('Rawaa2026!'),utcnow()))
        db.execute('INSERT INTO users(email,display,role,pw_hash,created_at) VALUES (?,?,?,?,?)',
                   ('operator@rawaa.local','Operations Contractor — Demo','operator',password_hash('Rawaa2026!'),utcnow()))
        # Geographic polygons are illustrative placeholders only, NOT official project borders.
        names = [('منطقة تجريبية A','Demo Zone A',21.548,39.164,'Sprinkler','Grass & trees'),
                 ('منطقة تجريبية B','Demo Zone B',21.574,39.184,'Drip','Shrubs & palms'),
                 ('منطقة تجريبية C','Demo Zone C',21.523,39.207,'Sprinkler','Grass'),
                 ('منطقة تجريبية D','Demo Zone D',21.601,39.156,'Drip','Trees & shrubs'),
                 ('منطقة تجريبية E','Demo Zone E',21.497,39.177,'Mixed','Palms & trees')]
        trends = [[.54,.55,.56,.54,.55,.56],[.56,.55,.53,.51,.50,.47],
                  [.55,.53,.49,.44,.40,.36],[.49,.48,.49,.50,.51,.52],[.52,.51,.50,.49,.48,.48]]
        thismonth = date.today().replace(day=1)
        for index,(ar,en,lat,lng,irrig,plants) in enumerate(names):
            cur = db.execute('INSERT INTO areas(name_ar,name_en,category,assigned_operator,geometry,irrigation,plants,planting_date,demo,created_at) VALUES (?,?,?,?,?,?,?,?,?,?)',
                (ar,en,'Green space',2,json.dumps(square(lng,lat)),irrig,plants,'2025-01-01',1,utcnow()))
            area_id=cur.lastrowid
            for monthidx,val in enumerate(trends[index]):
                d=(thismonth-timedelta(days=(5-monthidx)*30)).isoformat()
                db.execute('INSERT INTO observations(area_id,measured_at,ndvi,ndmi,valid_pixels_pct,source,is_demo,created_at) VALUES (?,?,?,?,?,?,?,?)',
                    (area_id,d,val,round(val-.16,3),91,'DEMO — synthetic sample',1,utcnow()))
        for area_id,severity,ar,en in [
            (2,'monitor','مؤشر نباتي منخفض مقارنة بالفترة السابقة','Vegetation index lower than baseline'),
            (3,'high','انخفاض متكرر في مؤشر الغطاء النباتي','Repeated vegetation-index decline')]:
            cur=db.execute('INSERT INTO alerts(area_id,severity,reason_ar,reason_en,status,created_at) VALUES (?,?,?,?,?,?)',
                (area_id,severity,ar,en,'new',utcnow()))
            db.execute('INSERT INTO tasks(area_id,alert_id,title_ar,title_en,assigned_to,status,priority,due_at,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?)',
                (area_id,cur.lastrowid,'زيارة تحقق ميداني','Field verification visit',2,'open','high' if severity=='high' else 'medium',
                 (date.today()+timedelta(days=4)).isoformat(),utcnow(),utcnow()))
        db.execute('INSERT INTO activity(user_id,verb,target,details,created_at) VALUES (?,?,?,?,?)',
            (1,'seed','system','Initial DEMO content — not satellite observations',utcnow()))

init_db()

# Real Jeddah sites. Boundaries marked "OSM" come from OpenStreetMap (© OpenStreetMap contributors, ODbL);
# "approx" boundaries are a ~200 x 150 m rectangle around the Google Maps pin until the real outline is drawn.
# No synthetic readings are attached to any of them: they only show what Copernicus Sentinel-2 returns.
OSM_RINGS = {"majid":[[39.181215,21.590235],[39.181659,21.589014],[39.185624,21.590207],[39.18501,21.592798],[39.184919,21.592876],[39.184829,21.592905],[39.183348,21.592545],[39.183229,21.59248],[39.181239,21.590371],[39.181215,21.590235]],"corniche":[[39.149704,21.522834],[39.149701,21.522635],[39.149728,21.522507],[39.150007,21.522122],[39.150252,21.521875],[39.150361,21.521794],[39.150511,21.521727],[39.15119,21.521505],[39.1513,21.521436],[39.151355,21.521365],[39.152012,21.519627],[39.152167,21.519267],[39.152226,21.519043],[39.152665,21.518426],[39.152866,21.518173],[39.152661,21.517818],[39.153044,21.516969],[39.15364,21.517187],[39.150853,21.523589],[39.150784,21.523905],[39.150796,21.524127],[39.150865,21.524408],[39.150988,21.524639],[39.151063,21.524735],[39.151344,21.525055],[39.151745,21.525448],[39.152315,21.525915],[39.152683,21.526192],[39.153174,21.526532],[39.15318,21.526572],[39.154136,21.527167],[39.154175,21.527159],[39.155497,21.527946],[39.15551,21.527984],[39.156067,21.528322],[39.156757,21.528705],[39.15676,21.528733],[39.157332,21.52898],[39.157876,21.529191],[39.157913,21.529185],[39.158147,21.529302],[39.158218,21.52935],[39.15838,21.529504],[39.158473,21.529635],[39.158575,21.529869],[39.158598,21.530067],[39.158183,21.530166],[39.158177,21.530225],[39.158098,21.530279],[39.157992,21.530266],[39.157987,21.530181],[39.157848,21.530187],[39.157835,21.530156],[39.157693,21.530124],[39.157425,21.530129],[39.156471,21.5302],[39.156411,21.530065],[39.156376,21.530081],[39.156225,21.529772],[39.156244,21.529761],[39.155648,21.528975],[39.155386,21.528813],[39.154762,21.528285],[39.15467,21.528175],[39.154682,21.528159],[39.154422,21.527936],[39.154206,21.527785],[39.154218,21.527762],[39.153956,21.527563],[39.153712,21.527413],[39.153724,21.527378],[39.153404,21.527125],[39.153234,21.526961],[39.153249,21.52694],[39.153078,21.526819],[39.152998,21.52672],[39.152918,21.526672],[39.152932,21.526653],[39.152803,21.526578],[39.152713,21.526461],[39.152591,21.526382],[39.152607,21.526356],[39.151663,21.525752],[39.151595,21.525482],[39.151556,21.525437],[39.151137,21.525127],[39.151104,21.525066],[39.151104,21.524996],[39.151077,21.524977],[39.151101,21.524953],[39.150889,21.524781],[39.150828,21.524693],[39.150843,21.524666],[39.150713,21.5245],[39.150717,21.524466],[39.150605,21.524402],[39.150529,21.524337],[39.150493,21.524193],[39.150531,21.523837],[39.150511,21.523778],[39.150104,21.523056],[39.149763,21.522898],[39.149704,21.522834]],"saja":[[39.185792,21.527631],[39.185831,21.527557],[39.185896,21.527514],[39.18683,21.527534],[39.186851,21.527514],[39.186968,21.524557],[39.188075,21.524599],[39.187865,21.528056],[39.187851,21.528091],[39.187817,21.52811],[39.187524,21.528072],[39.185896,21.527783],[39.185842,21.527749],[39.185803,21.527685],[39.185792,21.527631]],"tihamah":[[39.208188,21.54982],[39.208477,21.548486],[39.209049,21.548591],[39.209059,21.548697],[39.209095,21.548739],[39.209153,21.54876],[39.209242,21.548736],[39.209301,21.548638],[39.209885,21.548747],[39.209599,21.550091],[39.208188,21.54982]],"ajwad":[[39.252292,21.59362],[39.252354,21.592868],[39.2525,21.592229],[39.252517,21.592047],[39.252494,21.591656],[39.25255,21.5916],[39.252588,21.591598],[39.25338,21.591739],[39.253421,21.591784],[39.253433,21.591852],[39.253168,21.592939],[39.253155,21.593271],[39.25318,21.59345],[39.25329,21.59381],[39.253533,21.594281],[39.253781,21.594687],[39.253946,21.595058],[39.254068,21.595391],[39.254181,21.595872],[39.254213,21.596361],[39.254187,21.596388],[39.253942,21.596309],[39.253429,21.595899],[39.25303,21.595438],[39.252717,21.595013],[39.252377,21.594372],[39.252292,21.59362]]}

def _poly(key=None, lat=None, lng=None):
    if key: return {'type':'Polygon','coordinates':[OSM_RINGS[key]]}
    return square(lng,lat,0.001)

# (name_en key, name_ar, category, role_ar, role_en, geometry, boundary source)
REAL_SITES = [
    # Five case-study parks (one per case type in the project plan)
    ('Prince Majid Park','حديقة الأمير ماجد','Case study',
     'حالة ١ · حديقة مرجعية ممتازة: شكل NDVI الطبيعي (حي الربوة، نحو ٨ كم من البحر)',
     'Case 1 · Excellent reference park: normal NDVI shape (Ar Rabwah, ~8 km from the sea)',_poly('majid'),'OSM way 1074269189'),
    ('Al Huda Garden','حديقة الهدى','Case study',
     'حالة ٢ · مرشّحة لإجهاد مائي/موت نباتات (حي المروة). الترشيح من تقييمات الزوار العامة ويحتاج تأكيد الأمانة',
     'Case 2 · Candidate water stress / plant loss (Al Marwah). Based on public visitor reviews; needs municipality confirmation',_poly(lat=21.6105335,lng=39.2171712),'approx'),
    ('Middle Corniche Park','حديقة الكورنيش الأوسط','Case study',
     'حالة ٣ · ساحلية ملاصقة للبحر: اختبار الملوحة (حي الحمراء)',
     'Case 3 · Coastal, on the seafront: salinity test (Al Hamra)',_poly('corniche'),'OSM way 628514686'),
    ('Al Saja Gardens','حدائق السجى','Case study',
     'حالة ٤ · داخلية على نفس خط العرض وتبعد نحو ٤ كم عن البحر: شاهد (Control) للحالة ٣ (حي الشرفية)',
     'Case 4 · Inland at the same latitude, ~4 km from the sea: control for case 3 (Ash Sharafiyah)',_poly('saja'),'OSM way 165769855'),
    ('Abruq Al-Rughamah Garden','حديقة أبرق الرغامة','Case study',
     'حالة ٥ · مرشّحة لإجهاد حيوي/تشبع مائي وإهمال (حي أبرق الرغامة). الترشيح من تقييمات الزوار العامة ويحتاج تأكيد الأمانة',
     'Case 5 · Candidate biotic stress / waterlogging and neglect (Abruq Al-Rughamah). Based on public reviews; needs municipality confirmation',_poly(lat=21.5008814,lng=39.2719418),'approx'),
    # Field-validation sites supplied by the team, to compare with the municipality's own records
    ('Validation site 1','حديقة تهامة — موقع التحقق ١','Validation site',
     'موقع تحقق للمقارنة مع نتائج الأمانة (حي العزيزية)','Validation site for comparison with municipality results (Al Aziziyah)',_poly('tihamah'),'OSM way 628578775'),
    ('Validation site 2','موقع التحقق ٢','Validation site',
     'موقع تحقق للمقارنة مع نتائج الأمانة (حي الشاطئ، قرب الساحل)','Validation site for comparison with municipality results (Ash Shati, near the coast)',_poly(lat=21.6531027,lng=39.1083515),'approx'),
    ('Al Ajwad New Park','حديقة الأجواد الجديدة','Validation site',
     'موقع تحقق للمقارنة مع نتائج الأمانة (حي الأجواد)','Validation site for comparison with municipality results (Al Ajwad)',_poly('ajwad'),'OSM way 1491780911'),
]
SITE_INFO = {r[0]:{'role_ar':r[3],'role_en':r[4],'boundary_source':r[6]} for r in REAL_SITES}

def ensure_validation_sites():
    with connect() as db:
        for en,ar,cat,_ra,_re,geom,src in REAL_SITES:
            row=db.execute('SELECT id FROM areas WHERE name_en=?',(en,)).fetchone()
            if row:
                db.execute('UPDATE areas SET name_ar=?,category=?,geometry=? WHERE id=?',(ar,cat,json.dumps(geom),row[0]))
                continue
            db.execute('INSERT INTO areas(name_ar,name_en,category,assigned_operator,geometry,irrigation,plants,planting_date,demo,created_at) VALUES (?,?,?,?,?,?,?,?,?,?)',
                (ar,en,cat,2,json.dumps(geom),'','',None,0,utcnow()))
            db.execute('INSERT INTO activity(user_id,verb,target,details,created_at) VALUES (?,?,?,?,?)',
                (1,'seed','area',f'{cat} added: {en} - boundary: {src}',utcnow()))

ensure_validation_sites()

def purge_demo_areas():
    """Remove the synthetic demo zones (A-E) with their readings, alerts and tasks; only real sites remain.
    Set RAWAA_KEEP_DEMO=1 to keep them (used by the automated tests)."""
    if os.getenv('RAWAA_KEEP_DEMO')=='1': return
    with connect() as db:
        ids=[r[0] for r in db.execute('SELECT id FROM areas WHERE demo=1')]
        if not ids: return
        q=','.join('?'*len(ids))
        db.execute(f'DELETE FROM task_photos WHERE task_id IN (SELECT id FROM tasks WHERE area_id IN ({q}))',ids)
        db.execute(f'DELETE FROM tasks WHERE area_id IN ({q})',ids)
        db.execute(f'DELETE FROM alerts WHERE area_id IN ({q})',ids)
        db.execute(f'DELETE FROM observations WHERE area_id IN ({q}) OR is_demo=1',ids)
        db.execute(f'DELETE FROM areas WHERE id IN ({q})',ids)

purge_demo_areas()

EXTRA_INDEX_COLUMNS = ('ndwi','savi','ndre','salinity','ndvi_std')
def migrate_spectral_columns():
    with connect() as db:
        have={r[1] for r in db.execute('PRAGMA table_info(observations)')}
        for col in EXTRA_INDEX_COLUMNS:
            if col not in have: db.execute(f'ALTER TABLE observations ADD COLUMN {col} REAL')

migrate_spectral_columns()


def current_user(req: Request):
    cookie=req.cookies.get('rawaa_session','')
    if not cookie: raise HTTPException(401,'Login required')
    digest=hashlib.sha256(cookie.encode()).hexdigest()
    with connect() as db:
        r=db.execute('''SELECT u.id,u.email,u.display,u.role FROM sessions s
            JOIN users u ON s.user_id=u.id WHERE s.token_hash=? AND s.expires>?''',
            (digest,int(time.time()))).fetchone()
    if not r: raise HTTPException(401,'Session expired')
    return dict(r)

def require_regulator(u):
    if u['role']!='regulator': raise HTTPException(403,'Regulator role required')

def area_access(db, user, area_id):
    area=db.execute('SELECT * FROM areas WHERE id=?', (area_id,)).fetchone()
    if not area: raise HTTPException(404,'Area not found')
    if user['role']=='operator' and area['assigned_operator']!=user['id']:
        raise HTTPException(403,'Area outside your scope')
    return area


def in_scope_clause(user, alias='a'):
    if user['role']=='operator': return (f' WHERE {alias}.assigned_operator=?', [user['id']])
    return ('',[])


def metrics(area_id, db):
    rows=[dict(r) for r in db.execute('SELECT measured_at,ndvi,ndmi,ndwi,savi,ndre,salinity,ndvi_std,valid_pixels_pct,source,is_demo FROM observations WHERE area_id=? ORDER BY measured_at', (area_id,))]
    real=[r for r in rows if not r['is_demo'] and r['ndvi'] is not None]
    series=real if real else [r for r in rows if r['is_demo']]
    quality=[r for r in series if r['valid_pixels_pct'] >= 60]
    # A minimum of 3 valid chronological observations is required for even a provisional label.
    if len(quality)<3:
        return {'status':'insufficient','baseline':None,'current_ndvi':quality[-1]['ndvi'] if quality else None,
                'change_pct':None,'forecast':None,'forecast_kind':None,
                'latest_date':quality[-1]['measured_at'] if quality else None,'demo':bool(series and series[0]['is_demo']),
                'source':series[-1]['source'] if series else None,'observations':series}
    base=statistics.median([r['ndvi'] for r in quality[:-2]])
    latest=quality[-1]
    change_pct=(latest['ndvi']-base)/abs(base)*100 if base else None
    last2=[(r['ndvi']-base)/abs(base)*100 for r in quality[-2:]] if base else [0,0]
    if all(x<=-20 for x in last2): status='high'
    elif (change_pct is not None and change_pct<=-10): status='monitor'
    else: status='stable'
    # Simple descriptive extrapolation is NOT presented as a trained AI forecast.
    xs=[r['ndvi'] for r in quality[-3:]]
    monthly_delta=(xs[-1]-xs[0])/2
    forecast=[round(max(-1,min(1,latest['ndvi']+monthly_delta*i)),3) for i in (1,2)]
    return {'status':status,'baseline':round(base,3),'current_ndvi':latest['ndvi'],
            'change_pct':round(change_pct,1) if change_pct is not None else None,
            'forecast':forecast,'forecast_kind':'illustrative_linear_trend',
            'latest_date':latest['measured_at'],'demo':bool(series[0]['is_demo']),
            'source':latest['source'],'observations':series}


def full_area(db,r):
    d=dict(r)
    d['geometry']=json.loads(d['geometry'])
    d['metrics']=metrics(d['id'],db)
    d.update(SITE_INFO.get(d['name_en'],{}))
    return d

class Login(BaseModel):
    email:str; password:str

@app.get('/')
def home(): return FileResponse(ROOT / 'static' / 'index.html')

@app.post('/api/login')
def login(v:Login, response:Response):
    with connect() as db:
        u=db.execute('SELECT * FROM users WHERE email=?',(v.email.lower().strip(),)).fetchone()
        if not u or not password_ok(v.password,u['pw_hash']): raise HTTPException(401,'Invalid credentials')
        token=secrets.token_urlsafe(40)
        db.execute('INSERT INTO sessions(user_id,token_hash,expires) VALUES (?,?,?)',
                   (u['id'],hashlib.sha256(token.encode()).hexdigest(),int(time.time())+86400))
    response.set_cookie('rawaa_session',token,httponly=True,samesite='lax',secure=os.getenv('RAWAA_SECURE_COOKIE')=='1',max_age=86400)
    return {'user':{'id':u['id'],'email':u['email'],'display':u['display'],'role':u['role']}}

@app.post('/api/logout')
def logout(req:Request,response:Response):
    token=req.cookies.get('rawaa_session','')
    if token:
        with connect() as db:
            db.execute('DELETE FROM sessions WHERE token_hash=?',(hashlib.sha256(token.encode()).hexdigest(),))
    response.delete_cookie('rawaa_session')
    return {'ok':True}

class DemoRoleChange(BaseModel):
    role: str


def demo_switch_enabled():
    # Explicitly disabled when deployed in production; never a real permission elevation feature.
    return os.getenv('RAWAA_ENV', 'development').lower() != 'production' and os.getenv('RAWAA_DEMO_SWITCH', '1') == '1'


@app.post('/api/demo/switch-role')
def switch_demo_role(v:DemoRoleChange, req:Request, response:Response):
    user = current_user(req)
    demo_emails = {'regulator': 'regulator@rawaa.local', 'operator': 'operator@rawaa.local'}
    if not demo_switch_enabled() or user['email'] not in demo_emails.values():
        raise HTTPException(403, 'Demo role switching is disabled for this account')
    if v.role not in demo_emails:
        raise HTTPException(400, 'Invalid demo role')
    with connect() as db:
        target = db.execute('SELECT id,email,display,role FROM users WHERE email=? AND role=?',
                            (demo_emails[v.role], v.role)).fetchone()
        if not target: raise HTTPException(404, 'Demo account not found')
        prev_token = req.cookies.get('rawaa_session', '')
        if prev_token:
            db.execute('DELETE FROM sessions WHERE token_hash=?', (hashlib.sha256(prev_token.encode()).hexdigest(),))
        token = secrets.token_urlsafe(40)
        db.execute('INSERT INTO sessions(user_id,token_hash,expires) VALUES (?,?,?)',
                   (target['id'], hashlib.sha256(token.encode()).hexdigest(), int(time.time()) + 86400))
    response.set_cookie('rawaa_session', token, httponly=True, samesite='lax',
                        secure=os.getenv('RAWAA_SECURE_COOKIE') == '1', max_age=86400)
    return {'user': dict(target), 'demo_only': True}


@app.get('/api/me')
def me(req:Request): return current_user(req)

@app.get('/api/dashboard')
def dashboard(req:Request):
    u=current_user(req)
    clause,params=in_scope_clause(u)
    with connect() as db:
        areas=[full_area(db,r) for r in db.execute('SELECT a.* FROM areas a'+clause+' ORDER BY a.id',params)]
        tasks=[dict(r) for r in db.execute('SELECT t.*, EXISTS(SELECT 1 FROM task_photos p WHERE p.task_id=t.id AND p.kind="before") AS has_before_photo, EXISTS(SELECT 1 FROM task_photos p WHERE p.task_id=t.id AND p.kind="after") AS has_after_photo FROM tasks t JOIN areas a ON t.area_id=a.id'+clause+' ORDER BY t.created_at DESC',params)]
        alerts=[dict(r) for r in db.execute('SELECT al.* FROM alerts al JOIN areas a ON al.area_id=a.id'+clause+' ORDER BY al.created_at DESC',params)]
    return {'areas':areas,'tasks':tasks,'alerts':alerts,
        'counts':{'total':len(areas),'stable':sum(a['metrics']['status']=='stable' for a in areas),
                  'monitor':sum(a['metrics']['status']=='monitor' for a in areas),
                  'high':sum(a['metrics']['status']=='high' for a in areas),
                  'insufficient':sum(a['metrics']['status']=='insufficient' for a in areas),
                  'alerts':sum(a['status']!='closed' for a in alerts),
                  'tasks_open':sum(t['status'] not in ('closed',) for t in tasks)},
        'demo':any(a['metrics']['demo'] for a in areas),
        'integration':bool(os.getenv('COPERNICUS_CLIENT_ID') and os.getenv('COPERNICUS_CLIENT_SECRET')),
        'role_switch_enabled': demo_switch_enabled() and u['email'] in ('regulator@rawaa.local','operator@rawaa.local')}

@app.get('/api/areas')
def areas_list(req:Request):
    u=current_user(req);clause,params=in_scope_clause(u)
    with connect() as db:
        return [full_area(db,r) for r in db.execute('SELECT a.* FROM areas a'+clause+' ORDER BY a.id',params)]

class AreaCreate(BaseModel):
    name_ar:str=Field(min_length=2,max_length=120)
    name_en:str=Field(min_length=2,max_length=120)
    geometry:dict
    irrigation:str=''; plants:str=''; planting_date:str|None=None
    assigned_operator:int=2

def validate_geometry(g):
    if g.get('type')!='Polygon': raise HTTPException(400,'Geometry must be a GeoJSON Polygon')
    try:
        rings=g['coordinates']
        assert isinstance(rings,list) and len(rings)>=1 and len(rings[0])>=4
        outer=rings[0]
        assert outer[0]==outer[-1], 'Polygon ring must be closed'
        for ring in rings:
            assert len(ring)>=4 and ring[0]==ring[-1]
            for p in ring:
                assert isinstance(p,list) and len(p)>=2
                assert -180<=float(p[0])<=180 and -90<=float(p[1])<=90
    except (ValueError,TypeError,KeyError,AssertionError,IndexError):
        raise HTTPException(400,'Invalid closed polygon coordinates')

@app.post('/api/areas',status_code=201)
def add_area(v:AreaCreate,req:Request):
    u=current_user(req);require_regulator(u);validate_geometry(v.geometry)
    with connect() as db:
        assigned=db.execute('SELECT id FROM users WHERE id=? AND role="operator"',(v.assigned_operator,)).fetchone()
        if not assigned: raise HTTPException(400,'Operator not found')
        c=db.execute('''INSERT INTO areas(name_ar,name_en,geometry,irrigation,plants,planting_date,assigned_operator,demo,created_at)
            VALUES(?,?,?,?,?,?,?,?,?)''',(v.name_ar,v.name_en,json.dumps(v.geometry),v.irrigation,v.plants,v.planting_date,v.assigned_operator,0,utcnow()))
        db.execute('INSERT INTO activity(user_id,verb,target,details,created_at) VALUES(?,?,?,?,?)',(u['id'],'create_area',str(c.lastrowid),v.name_en,utcnow()))
        return full_area(db,db.execute('SELECT * FROM areas WHERE id=?',(c.lastrowid,)).fetchone())

@app.get('/api/areas/{area_id}')
def get_area(area_id:int,req:Request):
    u=current_user(req)
    with connect() as db: return full_area(db,area_access(db,u,area_id))

class CSVImport(BaseModel):
    csv_text:str=Field(min_length=10,max_length=1_000_000)

@app.post('/api/areas/{area_id}/import-csv')
def import_csv(area_id:int,v:CSVImport,req:Request):
    u=current_user(req);require_regulator(u)
    reader=csv.DictReader(io.StringIO(v.csv_text))
    wanted={'date','ndvi','ndmi','valid_pixels_pct'}
    if not reader.fieldnames or not {'date','ndvi','valid_pixels_pct'}.issubset(set(reader.fieldnames)):
        raise HTTPException(400,'Required columns: date,ndvi,valid_pixels_pct; optional: ndmi')
    clean=[]
    try:
        for row in reader:
            d=date.fromisoformat(row['date'].strip())
            ndvi=float(row['ndvi']);quality=float(row['valid_pixels_pct'])
            ndmi=float(row['ndmi']) if row.get('ndmi','').strip() else None
            if not (-1<=ndvi<=1 and 0<=quality<=100 and (ndmi is None or -1<=ndmi<=1)):
                raise ValueError('Out-of-range index or valid percentage')
            clean.append((d.isoformat(),ndvi,ndmi,quality))
        if not clean or len(clean)>2000: raise ValueError('CSV must contain 1–2000 rows')
    except (ValueError,TypeError,AttributeError,KeyError) as e:
        raise HTTPException(400,f'Invalid CSV row: {e}')
    with connect() as db:
        area_access(db,u,area_id)
        for d,ndvi,ndmi,q in clean:
            db.execute('''INSERT INTO observations(area_id,measured_at,ndvi,ndmi,valid_pixels_pct,source,is_demo,created_at)
                VALUES(?,?,?,?,?,'Imported CSV (user supplied)',0,?)
                ON CONFLICT(area_id,measured_at,source) DO UPDATE SET ndvi=excluded.ndvi,
                ndmi=excluded.ndmi,valid_pixels_pct=excluded.valid_pixels_pct''',
                (area_id,d,ndvi,ndmi,q,utcnow()))
        db.execute('INSERT INTO activity(user_id,verb,target,details,created_at) VALUES(?,?,?,?,?)',
                   (u['id'],'import_csv',str(area_id),f'{len(clean)} user-supplied records',utcnow()))
    return {'imported':len(clean),'note':'Imported values are user-provided; provenance must be confirmed separately.'}

@app.get('/api/alerts')
def list_alerts(req:Request):
    u=current_user(req);clause,params=in_scope_clause(u)
    with connect() as db:
        return [dict(r) for r in db.execute('SELECT al.* FROM alerts al JOIN areas a ON a.id=al.area_id'+clause+' ORDER BY al.id DESC',params)]

class AssignAlert(BaseModel):
    due_at: str|None=None
    priority: str='medium'

@app.post('/api/alerts/{alert_id}/assign')
def assign_alert(alert_id:int,v:AssignAlert,req:Request):
    u=current_user(req);require_regulator(u)
    if v.priority not in ('low','medium','high'): raise HTTPException(400,'Invalid priority')
    with connect() as db:
        a=db.execute('''SELECT al.*, ar.assigned_operator FROM alerts al
            JOIN areas ar ON ar.id=al.area_id WHERE al.id=?''',(alert_id,)).fetchone()
        if not a: raise HTTPException(404,'Alert not found')
        if not a['assigned_operator']: raise HTTPException(400,'No operator assigned to this area')
        dup=db.execute('SELECT id FROM tasks WHERE alert_id=? AND status!="closed"',(alert_id,)).fetchone()
        if dup: raise HTTPException(409,'Active task already exists for this alert')
        c=db.execute('''INSERT INTO tasks(area_id,alert_id,title_ar,title_en,assigned_to,status,priority,due_at,created_at,updated_at)
            VALUES(?,?,?,?,?,?,?,?,?,?)''',(a['area_id'],alert_id,'تحقق وصيانة','Inspection & maintenance',
             a['assigned_operator'],'open',v.priority,v.due_at,utcnow(),utcnow()))
        db.execute('UPDATE alerts SET status="assigned" WHERE id=?',(alert_id,))
        db.execute('INSERT INTO activity(user_id,verb,target,details,created_at) VALUES(?,?,?,?,?)',
                   (u['id'],'assign_task',str(c.lastrowid),f'alert {alert_id}',utcnow()))
    return {'id':c.lastrowid}

@app.get('/api/tasks')
def list_tasks(req:Request):
    u=current_user(req)
    with connect() as db:
        if u['role']=='operator':
            rows=db.execute('SELECT t.*, EXISTS(SELECT 1 FROM task_photos p WHERE p.task_id=t.id AND p.kind="before") AS has_before_photo, EXISTS(SELECT 1 FROM task_photos p WHERE p.task_id=t.id AND p.kind="after") AS has_after_photo FROM tasks t WHERE assigned_to=? ORDER BY id DESC',(u['id'],))
        else: rows=db.execute('SELECT t.*, EXISTS(SELECT 1 FROM task_photos p WHERE p.task_id=t.id AND p.kind="before") AS has_before_photo, EXISTS(SELECT 1 FROM task_photos p WHERE p.task_id=t.id AND p.kind="after") AS has_after_photo FROM tasks t ORDER BY id DESC')
        return [dict(r) for r in rows]

class TaskPatch(BaseModel):
    status:str|None=None
    actual_cause:str|None=None
    action:str|None=None
    notes:str|None=None

@app.patch('/api/tasks/{task_id}')
def patch_task(task_id:int,v:TaskPatch,req:Request):
    u=current_user(req)
    with connect() as db:
        t=db.execute('SELECT * FROM tasks WHERE id=?',(task_id,)).fetchone()
        if not t: raise HTTPException(404,'Task not found')
        if u['role']=='operator' and t['assigned_to']!=u['id']:
            raise HTTPException(403,'Task is not assigned to you')
        allowed_ops={'open':{'in_progress'},'in_progress':{'pending_review'},'reopened':{'in_progress'},'pending_review':set(),'closed':set()}
        allowed_reg={'pending_review':{'closed','reopened'},'open':{'reopened'},'in_progress':{'reopened'},'closed':set(),'reopened':set()}
        fields={k:val for k,val in v.model_dump(exclude_none=True).items() if k!='status'}
        if v.status:
            allowed=(allowed_reg if u['role']=='regulator' else allowed_ops).get(t['status'],set())
            if v.status not in allowed: raise HTTPException(409,f'Invalid state transition: {t["status"]} -> {v.status}')
            if v.status=='pending_review' and not ((fields.get('actual_cause') or t['actual_cause']) and (fields.get('action') or t['action'])):
                raise HTTPException(400,'Actual cause and action are required for review')
            fields['status']=v.status
        if not fields: raise HTTPException(400,'No fields supplied')
        fields['updated_at']=utcnow()
        sql='UPDATE tasks SET '+', '.join(f'{k}=?' for k in fields)+' WHERE id=?'
        db.execute(sql,[*fields.values(),task_id])
        if v.status=='closed' and t['alert_id']:
            db.execute('UPDATE alerts SET status="closed" WHERE id=?',(t['alert_id'],))
        db.execute('INSERT INTO activity(user_id,verb,target,details,created_at) VALUES(?,?,?,?,?)',
                   (u['id'],'update_task',str(task_id),json.dumps(fields,ensure_ascii=False),utcnow()))
        return asdict(db.execute('SELECT * FROM tasks WHERE id=?',(task_id,)).fetchone())

@app.get('/api/activity')
def activity(req:Request):
    u=current_user(req);require_regulator(u)
    with connect() as db:
        return [dict(r) for r in db.execute('SELECT * FROM activity ORDER BY id DESC LIMIT 100')]

@app.get('/api/reports/tasks.csv')
def export_tasks(req:Request):
    u=current_user(req)
    with connect() as db:
        if u['role']=='operator':
            rows=[dict(r) for r in db.execute('SELECT * FROM tasks WHERE assigned_to=?',(u['id'],))]
        else: rows=[dict(r) for r in db.execute('SELECT * FROM tasks')]
    out=io.StringIO();fields=['id','area_id','alert_id','title_en','status','priority','due_at','actual_cause','action','created_at','updated_at']
    writer=csv.DictWriter(out,fieldnames=fields,extrasaction='ignore');writer.writeheader();writer.writerows(rows)
    return Response(out.getvalue(),media_type='text/csv; charset=utf-8',headers={'Content-Disposition':'attachment; filename="rawaa-tasks.csv"'})

# Months in the approved maintenance programme (Appendix 6) with work that can briefly lower NDVI
# (pruning, frond/offshoot removal, lawn aeration/de-thatching/top dressing, annuals replacement) or raise it (fertiliser).
MAINT_DROP_MONTHS = {1,2,3,5,9,10,11,12}
MAINT_FERT_MONTHS = {1,3,5,9,11}

@app.get('/api/reports/dataset.csv')
def export_dataset(req:Request, include_demo:int=0):
    """Per-site monthly spectral features for analysis / model training. Real Sentinel-2 rows only by default."""
    u=current_user(req);require_regulator(u)
    with connect() as db:
        areas={r['id']:dict(r) for r in db.execute('SELECT * FROM areas')}
        rows=[dict(r) for r in db.execute('SELECT * FROM observations ORDER BY area_id,measured_at')]
    out=io.StringIO()
    fields=['area_id','name_en','name_ar','category','role_en','boundary_source','lat','lng','date','month',
            'ndvi','ndvi_std','ndmi','ndwi','savi','ndre','salinity','valid_pixels_pct','maint_drop_month','maint_fert_month','source','is_demo']
    w=csv.DictWriter(out,fieldnames=fields,extrasaction='ignore');w.writeheader()
    for r in rows:
        if r['is_demo'] and not include_demo: continue
        a=areas.get(r['area_id']);
        if not a: continue
        ring=json.loads(a['geometry'])['coordinates'][0]
        xs=[float(p[0]) for p in ring];ys=[float(p[1]) for p in ring]
        m=int(r['measured_at'][5:7]);info=SITE_INFO.get(a['name_en'],{})
        w.writerow({**r,'name_en':a['name_en'],'name_ar':a['name_ar'],'category':a['category'],'role_en':info.get('role_en',''),
                    'boundary_source':info.get('boundary_source',''),'lat':round((min(ys)+max(ys))/2,6),'lng':round((min(xs)+max(xs))/2,6),
                    'date':r['measured_at'],'month':m,'maint_drop_month':int(m in MAINT_DROP_MONTHS),'maint_fert_month':int(m in MAINT_FERT_MONTHS)})
    return Response('\ufeff'+out.getvalue(),media_type='text/csv; charset=utf-8',headers={'Content-Disposition':'attachment; filename="rawaa-training-dataset.csv"'})

# --- Copernicus Data Space — Sentinel Hub Statistical API, optional OAuth integration ---
EVALSCRIPT = '''//VERSION=3
// Monthly spectral features for each site. SCL mask removes no-data, saturated, cloud shadow, water, clouds, cirrus, snow.
function setup() {
  return { input: [{bands:["B02","B03","B04","B05","B08","B8A","B11","SCL","dataMask"]}],
    output: [ {id:"ndvi",bands:1,sampleType:"FLOAT32"}, {id:"ndmi",bands:1,sampleType:"FLOAT32"},
              {id:"ndwi",bands:1,sampleType:"FLOAT32"}, {id:"savi",bands:1,sampleType:"FLOAT32"},
              {id:"ndre",bands:1,sampleType:"FLOAT32"}, {id:"salinity",bands:1,sampleType:"FLOAT32"},
              {id:"valid",bands:1,sampleType:"FLOAT32"}, {id:"dataMask",bands:1} ] };
}
function nd(a,b){ return (a+b)!==0 ? (a-b)/(a+b) : 0; }
function evaluatePixel(s) {
  let good = s.dataMask===1 && ![0,1,3,6,8,9,10,11].includes(s.SCL);
  // dataMask = pixel inside the park polygon; masked (cloud etc.) pixels return NaN so they are excluded
  // from index statistics, while "valid" gives the clear-sky share of the park itself.
  let N = NaN;
  let savi = (s.B08+s.B04+0.5)!==0 ? 1.5*(s.B08-s.B04)/(s.B08+s.B04+0.5) : 0;
  return {ndvi:[good?nd(s.B08,s.B04):N], ndmi:[good?nd(s.B08,s.B11):N], ndwi:[good?nd(s.B03,s.B08):N], savi:[good?savi:N],
          ndre:[good?nd(s.B8A,s.B05):N], salinity:[good?Math.sqrt(Math.max(0,s.B02*s.B04)):N], valid:[good?1:0], dataMask:[s.dataMask]};
}
'''

def api_json(url, data, headers, timeout=40):
    payload=parse.urlencode(data).encode() if isinstance(data,dict) and headers.get('Content-Type','').startswith('application/x-www-form-urlencoded') else json.dumps(data).encode()
    req=request.Request(url,data=payload,headers=headers,method='POST')
    try:
        with request.urlopen(req,timeout=timeout) as res: return json.load(res)
    except error.HTTPError as e:
        msg=e.read(900).decode(errors='replace')
        raise RuntimeError(f'Copernicus HTTP {e.code}: {msg}') from e
    except error.URLError as e:
        raise RuntimeError(f'Copernicus network error: {e.reason}') from e

@app.get('/api/integrations')
def integrations(req:Request):
    current_user(req)
    return {'copernicus_configured':bool(os.getenv('COPERNICUS_CLIENT_ID') and os.getenv('COPERNICUS_CLIENT_SECRET')),
            'source':'Copernicus Data Space / Sentinel Hub Statistical API',
            'required':'COPERNICUS_CLIENT_ID and COPERNICUS_CLIENT_SECRET on the server',
            'mode':'Real satellite statistics only when configured; otherwise sample data is DEMO.'}

class Sync(BaseModel):
    start_date:str|None=None; end_date:str|None=None

# --- Optional live Sentinel-2 imagery for web map, never exposed as a public credential ---
TRUE_COLOR_SCRIPT = """//VERSION=3
function setup(){return {input:[{bands:['B04','B03','B02','SCL','dataMask']}],output:{bands:4}}}
function evaluatePixel(s){
  if(s.dataMask===0 || [0,1,3,8,9,10,11].includes(s.SCL))return [0,0,0,0];
  return [Math.min(1,2.7*s.B04),Math.min(1,2.7*s.B03),Math.min(1,2.7*s.B02),1];
}
"""
NDVI_IMAGE_SCRIPT = """//VERSION=3
function setup(){return {input:[{bands:['B04','B08','SCL','dataMask']}],output:{bands:4}}}
function evaluatePixel(s){
  if(s.dataMask===0 || [0,1,3,6,8,9,10,11].includes(s.SCL))return [0,0,0,0];
  let v=(s.B08+s.B04)>0?(s.B08-s.B04)/(s.B08+s.B04):0;
  if(v>=0.55)return [0.08,0.50,0.23,0.85];
  if(v>=0.35)return [0.27,0.71,0.36,0.85];
  if(v>=0.20)return [0.92,0.77,0.23,0.85];
  if(v>=0.08)return [0.90,0.44,0.15,0.85];
  return [0.74,0.19,0.17,0.85];
}
"""
_MASK = "if(s.dataMask===0 || [0,1,3,8,9,10,11].includes(s.SCL)) return [0,0,0,0];"
def _layer_script(bands, body):
    return ("//VERSION=3\nfunction setup(){return {input:[{bands:%s}],output:{bands:4}}}\n"
            "function nd(a,b){return (a+b)!==0?(a-b)/(a+b):0}\n"
            "function evaluatePixel(s){%s %s}" % (json.dumps(bands+['SCL','dataMask']), _MASK, body))
LAYER_SCRIPTS = {
  'false_color': _layer_script(['B08','B04','B03'], "return [Math.min(1,2.5*s.B08),Math.min(1,2.5*s.B04),Math.min(1,2.5*s.B03),1];"),
  'urban': _layer_script(['B12','B11','B04'], "return [Math.min(1,2.5*s.B12),Math.min(1,2.5*s.B11),Math.min(1,2.5*s.B04),1];"),
  'swir': _layer_script(['B12','B8A','B04'], "return [Math.min(1,2.5*s.B12),Math.min(1,2.5*s.B8A),Math.min(1,2.5*s.B04),1];"),
  'moisture': _layer_script(['B8A','B11'], "let v=nd(s.B8A,s.B11);let c=colorBlend(v,[-0.8,-0.24,-0.032,0.032,0.24,0.8],[[0.5,0,0],[1,0,0],[1,1,0],[0,1,1],[0,0,1],[0,0,0.5]]);return [c[0],c[1],c[2],0.85];"),
  'ndwi': _layer_script(['B03','B08'], "let v=nd(s.B03,s.B08);let c=colorBlend(v,[-0.8,-0.3,0,0.2,0.5],[[0,0.4,0],[0.6,0.8,0.4],[1,1,1],[0.3,0.6,1],[0,0,0.6]]);return [c[0],c[1],c[2],0.85];"),
  'ndre': _layer_script(['B8A','B05'], "let v=nd(s.B8A,s.B05);let c=colorBlend(v,[0,0.15,0.3,0.45,0.6],[[0.74,0.19,0.17],[0.9,0.44,0.15],[0.92,0.77,0.23],[0.27,0.71,0.36],[0.08,0.5,0.23]]);return [c[0],c[1],c[2],0.85];"),
}

class ImageryRequest(BaseModel):
    month:str=Field(pattern=r'^\d{4}-(0[1-9]|1[0-2])$')
    layer:str=Field(pattern=r'^(true_color|false_color|ndvi|moisture|ndwi|swir|urban|ndre)$')

_TOKEN_CACHE={'value':None,'expires':0.0}
_TOKEN_LOCK=threading.Lock()

def _copernicus_access_token():
    with _TOKEN_LOCK:
        if _TOKEN_CACHE['value'] and time.time()<_TOKEN_CACHE['expires']:
            return _TOKEN_CACHE['value']
        tok=_fetch_copernicus_token()
        _TOKEN_CACHE.update(value=tok,expires=time.time()+480)  # CDSE tokens live ~10 min
        return tok

def _fetch_copernicus_token():
    ident=os.getenv('COPERNICUS_CLIENT_ID')
    secret=os.getenv('COPERNICUS_CLIENT_SECRET')
    if not ident or not secret:
        raise HTTPException(422,'Copernicus not connected. Configure OAuth on the backend first.')
    return api_json('https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token',
                    {'grant_type':'client_credentials','client_id':ident,'client_secret':secret},
                    {'Content-Type':'application/x-www-form-urlencoded'})['access_token']

def _process_image(payload, token):
    req=request.Request('https://sh.dataspace.copernicus.eu/process/v1',
      data=json.dumps(payload).encode(),
      headers={'Content-Type':'application/json','Authorization':'Bearer '+token},method='POST')
    try:
        with request.urlopen(req,timeout=80) as r:
            b=r.read(2_100_000)
            if not r.headers.get('Content-Type','').startswith('image/png') or len(b)>2_000_000:
                raise RuntimeError('Unexpected imagery response')
            return b
    except error.HTTPError as exc:
        detail=exc.read(600).decode(errors='replace')
        raise RuntimeError(f'Copernicus HTTP {exc.code}: {detail}') from exc
    except error.URLError as exc:
        raise RuntimeError('Copernicus imagery service unavailable') from exc

@app.post('/api/areas/{area_id}/imagery')
def satellite_image(area_id:int,v:ImageryRequest,req:Request):
    u=current_user(req)
    with connect() as db:
        a=area_access(db,u,area_id)
        geom=json.loads(a['geometry'])
        validate_geometry(geom)
    try:
        period=date.fromisoformat(v.month+'-01')
    except ValueError:
        raise HTTPException(400,'Invalid month')
    if period>date.today().replace(day=1) or period.year<2018:
        raise HTTPException(400,'Month must be between 2018 and the current month')
    x=[float(p[0]) for ring in geom['coordinates'] for p in ring]
    y=[float(p[1]) for ring in geom['coordinates'] for p in ring]
    bbox=[min(x),min(y),max(x),max(y)]
    if bbox[2]-bbox[0] < 0.0001 or bbox[3]-bbox[1] < 0.0001 or bbox[2]-bbox[0] > 0.12 or bbox[3]-bbox[1] > 0.12:
        raise HTTPException(422,'Area must have a meaningful polygon no larger than 0.12 degrees in each direction')
    next_month=(period.replace(day=28)+timedelta(days=4)).replace(day=1)
    end=min(date.today(),next_month-timedelta(days=1))
    # True date-bounded imagery from Sentinel-2; if unavailable, provider will signal error.
    payload={
      'input':{'bounds':{'bbox':bbox,'geometry':geom,'properties':{'crs':'http://www.opengis.net/def/crs/OGC/1.3/CRS84'}},
                'data':[{'type':'sentinel-2-l2a','dataFilter':{'timeRange':{'from':period.isoformat()+'T00:00:00Z','to':end.isoformat()+'T23:59:59Z'},'mosaickingOrder':'leastCC'}}]},
      'output':{'width':512,'height':512,'responses':[{'identifier':'default','format':{'type':'image/png'}}]},
      'evalscript':NDVI_IMAGE_SCRIPT if v.layer=='ndvi' else TRUE_COLOR_SCRIPT if v.layer=='true_color' else LAYER_SCRIPTS[v.layer]}
    try:
        token=_copernicus_access_token()
        data=_process_image(payload,token)
    except RuntimeError as ex:
        raise HTTPException(502,str(ex)) from ex
    return {'image_data_url':'data:image/png;base64,'+base64.b64encode(data).decode('ascii'),
            'bounds':bbox,'month':v.month,'capture_date':None,'period_start':period.isoformat(),'period_end':end.isoformat(),'layer':v.layer,'provider':'Copernicus Data Space Sentinel Hub Process API',
            'note':'True Sentinel-2 L2A composite for requested period; not a single dated acquisition.'}

# --- Copernicus Browser-style basemap: Sentinel-2 L2A XYZ tiles rendered through the Process API ---
# Whole-scene rendering (clouds visible, like Copernicus Browser), restricted to the Jeddah region to protect quota.
def _map_script(bands, body):
    return ("//VERSION=3\nfunction setup(){return {input:[{bands:%s}],output:{bands:4}}}\n"
            "function nd(a,b){return (a+b)!==0?(a-b)/(a+b):0}\n"
            "function evaluatePixel(s){if(s.dataMask===0)return [0,0,0,0]; %s}" % (json.dumps(bands+['dataMask']), body))
_G=lambda b:'Math.min(1,2.5*s.%s)'%b
MAP_SCRIPTS={
  'true_color': _map_script(['B04','B03','B02'], f"return [{_G('B04')},{_G('B03')},{_G('B02')},1];"),
  'false_color': _map_script(['B08','B04','B03'], f"return [{_G('B08')},{_G('B04')},{_G('B03')},1];"),
  'urban': _map_script(['B12','B11','B04'], f"return [{_G('B12')},{_G('B11')},{_G('B04')},1];"),
  'swir': _map_script(['B12','B8A','B04'], f"return [{_G('B12')},{_G('B8A')},{_G('B04')},1];"),
  'ndvi': _map_script(['B04','B08'], "let v=nd(s.B08,s.B04);let c=colorBlend(v,[-0.5,-0.2,0,0.025,0.05,0.075,0.1,0.125,0.15,0.175,0.2,0.25,0.3,0.35,0.4,0.45,0.5,0.55,0.6,1],"
     "[[0.75,0.75,0.75],[0.86,0.86,0.86],[1,1,0.88],[1,0.98,0.8],[0.93,0.91,0.71],[0.87,0.85,0.61],[0.8,0.78,0.51],[0.74,0.72,0.42],[0.69,0.76,0.38],[0.64,0.8,0.35],"
     "[0.57,0.75,0.32],[0.5,0.7,0.28],[0.44,0.64,0.25],[0.38,0.59,0.21],[0.31,0.54,0.18],[0.25,0.49,0.14],[0.19,0.43,0.11],[0.13,0.38,0.07],[0.06,0.33,0.04],[0,0.27,0]]);return [c[0],c[1],c[2],1];"),
  'ndre': _map_script(['B8A','B05'], "let v=nd(s.B8A,s.B05);let c=colorBlend(v,[0,0.15,0.3,0.45,0.6],[[0.74,0.19,0.17],[0.9,0.44,0.15],[0.92,0.77,0.23],[0.27,0.71,0.36],[0.08,0.5,0.23]]);return [c[0],c[1],c[2],1];"),
  'moisture': _map_script(['B8A','B11'], "let v=nd(s.B8A,s.B11);let c=colorBlend(v,[-0.8,-0.24,-0.032,0.032,0.24,0.8],[[0.5,0,0],[1,0,0],[1,1,0],[0,1,1],[0,0,1],[0,0,0.5]]);return [c[0],c[1],c[2],1];"),
  'ndwi': _map_script(['B03','B08'], "let v=nd(s.B03,s.B08);let c=colorBlend(v,[-0.8,-0.3,0,0.2,0.5],[[0,0.4,0],[0.6,0.8,0.4],[1,1,1],[0.3,0.6,1],[0,0,0.6]]);return [c[0],c[1],c[2],1];"),
}
TILE_REGION=(38.95,21.15,39.45,21.95)   # lon/lat box around Jeddah
TILE_DIR=Path(os.getenv('RAWAA_TILE_CACHE','/tmp/rawaa-tiles'))
_EMPTY_PNG=base64.b64decode('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4XmNgYGBgAAAABQABGWkDzwAAAABJRU5ErkJggg==')
_O=20037508.342789244

def _tile_bounds(z,x,y):
    size=2*_O/(2**z);minx=-_O+x*size;maxy=_O-y*size
    return [minx,maxy-size,minx+size,maxy]

def _merc_to_lonlat(mx,my):
    return mx/_O*180, math.degrees(2*math.atan(math.exp(my/_O*math.pi))-math.pi/2)

@app.get('/api/tiles/{layer}/{z}/{x}/{y}.png')
def sentinel_tile(layer:str,z:int,x:int,y:int,req:Request,month:str='',cc:int=30):
    current_user(req)
    if layer not in MAP_SCRIPTS: raise HTTPException(404,'Unknown layer')
    if not (9<=z<=17) or not (0<=x<2**z and 0<=y<2**z): raise HTTPException(404,'Zoom out of range')
    if not re.fullmatch(r'\d{4}-(0[1-9]|1[0-2])',month or ''): raise HTTPException(400,'month=YYYY-MM required')
    cc=max(0,min(100,int(cc)))
    period=date.fromisoformat(month+'-01')
    if period>date.today().replace(day=1) or period.year<2017: raise HTTPException(400,'Month out of range')
    b=_tile_bounds(z,x,y);w,s_=_merc_to_lonlat(b[0],b[1]);e,n=_merc_to_lonlat(b[2],b[3])
    hdr={'Cache-Control':'private, max-age=86400'}
    if e<TILE_REGION[0] or w>TILE_REGION[2] or n<TILE_REGION[1] or s_>TILE_REGION[3]:
        return Response(_EMPTY_PNG,media_type='image/png',headers=hdr)
    path=TILE_DIR/layer/month/str(cc)/str(z)/str(x)/f'{y}.png'
    if path.exists(): return Response(path.read_bytes(),media_type='image/png',headers=hdr)
    next_month=(period.replace(day=28)+timedelta(days=4)).replace(day=1)
    end=min(date.today(),next_month-timedelta(days=1))
    payload={'input':{'bounds':{'bbox':b,'properties':{'crs':'http://www.opengis.net/def/crs/EPSG/0/3857'}},
             'data':[{'type':'sentinel-2-l2a','dataFilter':{'timeRange':{'from':period.isoformat()+'T00:00:00Z','to':end.isoformat()+'T23:59:59Z'},
                      'mosaickingOrder':'leastCC','maxCloudCoverage':cc}}]},
             'output':{'width':256,'height':256,'responses':[{'identifier':'default','format':{'type':'image/png'}}]},
             'evalscript':MAP_SCRIPTS[layer]}
    try:
        data=_process_image(payload,_copernicus_access_token())
    except RuntimeError as ex:
        raise HTTPException(502,str(ex)[:300]) from ex
    try:
        path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(data)
    except OSError:
        pass
    return Response(data,media_type='image/png',headers=hdr)

@app.post('/api/areas/{area_id}/sync-satellite')
def sync_satellite(area_id:int,v:Sync,req:Request):
    u=current_user(req);require_regulator(u)
    client_id=os.getenv('COPERNICUS_CLIENT_ID');secret=os.getenv('COPERNICUS_CLIENT_SECRET')
    if not client_id or not secret: raise HTTPException(422,'Configure Copernicus OAuth credentials in server environment first')
    end=date.fromisoformat(v.end_date) if v.end_date else date.today()
    start=date.fromisoformat(v.start_date) if v.start_date else end-timedelta(days=365)
    if start>=end or (end-start).days>730: raise HTTPException(400,'Date range must be 1–730 days')
    with connect() as db:
        a=area_access(db,u,area_id)
        polygon=json.loads(a['geometry'])
    bounds={'geometry':polygon,'properties':{'crs':'http://www.opengis.net/def/crs/EPSG/0/4326'}}
    try:
        tok=api_json('https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token',
            {'grant_type':'client_credentials','client_id':client_id,'client_secret':secret},
            {'Content-Type':'application/x-www-form-urlencoded'})['access_token']
        stat_req={'input':{'bounds':bounds,'data':[{'type':'sentinel-2-l2a','dataFilter':{'maxCloudCoverage':80,'mosaickingOrder':'mostRecent'}}]},
                  'aggregation':{'timeRange':{'from':f'{start}T00:00:00Z','to':f'{end}T23:59:59Z'},
                                 'aggregationInterval':{'of':'P30D'},'lastIntervalBehavior':'SHORTEN','evalscript':EVALSCRIPT,
                                 'resx':0.0001,'resy':0.0001}}
        results=api_json('https://sh.dataspace.copernicus.eu/statistics/v1',stat_req,
                          {'Content-Type':'application/json','Authorization':'Bearer '+tok},timeout=90)
    except RuntimeError as ex:
        raise HTTPException(502,str(ex))
    saved=0;skipped=0
    with connect() as db:
        for item in results.get('data',[]):
            outputs=item.get('outputs',{})
            v1=outputs.get('ndvi',{}).get('bands',{}).get('B0',{}).get('stats',{})
            v2=outputs.get('ndmi',{}).get('bands',{}).get('B0',{}).get('stats',{})
            ext={k:outputs.get(k,{}).get('bands',{}).get('B0',{}).get('stats',{}).get('mean') for k in ('ndwi','savi','ndre','salinity')}
            ext={k:(round(x,5) if isinstance(x,(int,float)) and math.isfinite(x) else None) for k,x in ext.items()}
            std=v1.get('stDev');std=round(std,5) if isinstance(std,(int,float)) and math.isfinite(std) else None
            vm=outputs.get('valid',{}).get('bands',{}).get('B0',{}).get('stats',{}).get('mean')
            if isinstance(vm,(int,float)) and math.isfinite(vm):
                pct=vm*100  # clear-sky share of pixels inside the park polygon
            else:
                count=v1.get('sampleCount',0);nodata=v1.get('noDataCount',count)
                pct=(count-nodata)/count*100 if count else 0
            vi=v1.get('mean');mi=v2.get('mean')
            if not (isinstance(mi,(int,float)) and math.isfinite(mi)): mi=None
            if not (isinstance(vi,(int,float)) and math.isfinite(vi)) or pct<60:
                skipped+=1;continue
            dt=item['interval']['from'][:10]
            db.execute('''INSERT INTO observations(area_id,measured_at,ndvi,ndmi,ndwi,savi,ndre,salinity,ndvi_std,valid_pixels_pct,source,is_demo,created_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,'Copernicus Sentinel-2 L2A',0,?)
                ON CONFLICT(area_id,measured_at,source) DO UPDATE SET
                ndvi=excluded.ndvi,ndmi=excluded.ndmi,ndwi=excluded.ndwi,savi=excluded.savi,ndre=excluded.ndre,
                salinity=excluded.salinity,ndvi_std=excluded.ndvi_std,valid_pixels_pct=excluded.valid_pixels_pct''',
                (area_id,dt,round(vi,5),round(mi,5) if mi is not None else None,ext['ndwi'],ext['savi'],ext['ndre'],ext['salinity'],std,round(pct,1),utcnow()))
            saved+=1
        db.execute('INSERT INTO activity(user_id,verb,target,details,created_at) VALUES(?,?,?,?,?)',
                   (u['id'],'satellite_sync',str(area_id),f'{saved} periods, {skipped} excluded',utcnow()))
    return {'saved_intervals':saved,'excluded_intervals':skipped,'source':'Copernicus Sentinel-2 L2A','note':'Each output is one composited 30-day interval, not a single-image acquisition.'}


@app.post('/api/areas/{area_id}/scan')
def scan_area(area_id:int,req:Request):
    """Evaluate the selected area using transparent provisional thresholds, no ML."""
    u=current_user(req);require_regulator(u)
    with connect() as db:
        a=area_access(db,u,area_id)
        result=metrics(area_id,db)
        if result['status'] not in ('high','monitor'):
            raise HTTPException(409,'No provisional alert threshold currently met')
        existing=db.execute('SELECT id FROM alerts WHERE area_id=? AND status!="closed"',(area_id,)).fetchone()
        if existing:
            raise HTTPException(409,'An active alert already exists for this area')
        cur=db.execute('''INSERT INTO alerts(area_id,severity,reason_ar,reason_en,status,created_at)
            VALUES(?,?,?,?,?,?)''',(area_id,result['status'],
              'انخفاض في مؤشر الغطاء النباتي (تصنيف تجريبي)',
              'Vegetation index decline (provisional classification)','new',utcnow()))
        db.execute('INSERT INTO activity(user_id,verb,target,details,created_at) VALUES (?,?,?,?,?)',
            (u['id'],'scan',str(area_id),f'alert {cur.lastrowid} — {result["status"]}',utcnow()))
        return {'alert_id':cur.lastrowid,'status':result['status'],'message':'Provisional alert created; field verification required'}

# Before/after task photo evidence. Files are stored outside /static and require authorization.
class PhotoUpload(BaseModel):
    kind: str
    data_url: str

def checked_task(db, user, task_id):
    task=db.execute('SELECT * FROM tasks WHERE id=?',(task_id,)).fetchone()
    if not task: raise HTTPException(404,'Task not found')
    if user['role']=='operator' and task['assigned_to']!=user['id']:
        raise HTTPException(403,'Task outside your scope')
    return task

@app.post('/api/tasks/{task_id}/photo')
def upload_photo(task_id:int,v:PhotoUpload,req:Request):
    u=current_user(req)
    if u['role']!='operator': raise HTTPException(403,'Only operator uploads photo evidence')
    if v.kind not in ('before','after'): raise HTTPException(400,'kind must be before or after')
    with connect() as db:
        task=checked_task(db,u,task_id)
        if task['status']=='closed': raise HTTPException(409,'Task is closed')
    try:
        header,body=v.data_url.split(',',1)
        if not header.startswith('data:image/') or ';base64' not in header:
            raise ValueError('Expected base64 image data URL')
        raw=base64.b64decode(body,validate=True)
    except (ValueError,binascii.Error) as ex:
        raise HTTPException(400,f'Invalid image encoding: {ex}')
    if len(raw)>3*1024*1024 or len(raw)<8: raise HTTPException(400,'Image size must be 8 bytes–3 MB')
    if raw.startswith(b'\x89PNG\r\n\x1a\n'):
        mime,ext='image/png','png'
    elif raw.startswith(b'\xff\xd8\xff'):
        mime,ext='image/jpeg','jpg'
    elif raw[:4]==b'RIFF' and raw[8:12]==b'WEBP':
        mime,ext='image/webp','webp'
    else: raise HTTPException(400,'Only valid PNG/JPEG/WebP images are supported')
    file_name=secrets.token_hex(18)+'.'+ext
    photos_dir=ROOT / 'media';photos_dir.mkdir(exist_ok=True)
    (photos_dir / file_name).write_bytes(raw)
    with connect() as db:
        old=db.execute('SELECT filename FROM task_photos WHERE task_id=? AND kind=?',(task_id,v.kind)).fetchone()
        db.execute('''INSERT INTO task_photos(task_id,kind,filename,mime,uploaded_by,created_at)
              VALUES(?,?,?,?,?,?) ON CONFLICT(task_id,kind) DO UPDATE SET
              filename=excluded.filename,mime=excluded.mime,uploaded_by=excluded.uploaded_by,created_at=excluded.created_at''',
              (task_id,v.kind,file_name,mime,u['id'],utcnow()))
        db.execute('INSERT INTO activity(user_id,verb,target,details,created_at) VALUES(?,?,?,?,?)',
                   (u['id'],'task_photo',str(task_id),v.kind,utcnow()))
    if old:
        try:(photos_dir / old['filename']).unlink(missing_ok=True)
        except OSError:pass
    return {'uploaded':True,'kind':v.kind}

@app.get('/api/tasks/{task_id}/photo/{kind}')
def get_photo(task_id:int,kind:str,req:Request):
    u=current_user(req)
    if kind not in ('before','after'): raise HTTPException(404,'Photo kind not found')
    with connect() as db:
        checked_task(db,u,task_id)
        r=db.execute('SELECT filename,mime FROM task_photos WHERE task_id=? AND kind=?',(task_id,kind)).fetchone()
    if not r: raise HTTPException(404,'Photo not found')
    path=ROOT / 'media' / r['filename']
    if not path.is_file(): raise HTTPException(404,'Photo file missing')
    return FileResponse(path,media_type=r['mime'])

if __name__=='__main__':
    import uvicorn
    uvicorn.run(app,host='127.0.0.1',port=8000)
