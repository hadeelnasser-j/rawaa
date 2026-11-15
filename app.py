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

# Field-validation sites supplied by the team (Google Maps pins). Boundaries are an approximate
# ~200 x 150 m rectangle around each pin until the real park outline is drawn. No synthetic readings
# are attached: these sites only show what Copernicus Sentinel-2 actually returns.
VALIDATION_SITES = [
    ('موقع التحقق ١','Validation site 1',21.5492043,39.2089827),
    ('موقع التحقق ٢','Validation site 2',21.6531027,39.1083515),
    ('حديقة الأجواد الجديدة','Al Ajwad New Park',21.5950333,39.2533068),
]

def ensure_validation_sites():
    with connect() as db:
        for ar,en,lat,lng in VALIDATION_SITES:
            if db.execute('SELECT 1 FROM areas WHERE name_en=?',(en,)).fetchone():
                continue
            db.execute('INSERT INTO areas(name_ar,name_en,category,assigned_operator,geometry,irrigation,plants,planting_date,demo,created_at) VALUES (?,?,?,?,?,?,?,?,?,?)',
                (ar,en,'Validation site',2,json.dumps(square(lng,lat,0.001)),'','',None,0,utcnow()))
            db.execute('INSERT INTO activity(user_id,verb,target,details,created_at) VALUES (?,?,?,?,?)',
                (1,'seed','area',f'Validation site added: {en} ({lat:.5f}, {lng:.5f}) - approximate boundary',utcnow()))

ensure_validation_sites()


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
    rows=[dict(r) for r in db.execute('SELECT measured_at,ndvi,ndmi,valid_pixels_pct,source,is_demo FROM observations WHERE area_id=? ORDER BY measured_at', (area_id,))]
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

# --- Copernicus Data Space — Sentinel Hub Statistical API, optional OAuth integration ---
EVALSCRIPT = '''//VERSION=3
function setup() {
  return { input: [{bands:["B04","B08","B11","SCL","dataMask"], units:"REFLECTANCE"}],
    output: [ {id:"ndvi",bands:1,sampleType:"FLOAT32"},
              {id:"ndmi",bands:1,sampleType:"FLOAT32"},
              {id:"dataMask",bands:1} ] };
}
function evaluatePixel(s) {
  let good = s.dataMask===1 && [4,5].includes(s.SCL);
  let vi = (s.B08+s.B04)>0 ? (s.B08-s.B04)/(s.B08+s.B04) : 0;
  let mi = (s.B08+s.B11)>0 ? (s.B08-s.B11)/(s.B08+s.B11) : 0;
  return {ndvi:[vi],ndmi:[mi],dataMask:[good?1:0]};
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
function setup(){return {input:[{bands:['B04','B03','B02','SCL','dataMask'],units:'REFLECTANCE'}],output:{bands:4}}}
function evaluatePixel(s){
  if(s.dataMask===0 || [0,1,3,8,9,10,11].includes(s.SCL))return [0,0,0,0];
  return [Math.min(1,2.7*s.B04),Math.min(1,2.7*s.B03),Math.min(1,2.7*s.B02),1];
}
"""
NDVI_IMAGE_SCRIPT = """//VERSION=3
function setup(){return {input:[{bands:['B04','B08','SCL','dataMask'],units:'REFLECTANCE'}],output:{bands:4}}}
function evaluatePixel(s){
  if(s.dataMask===0 || ![4,5].includes(s.SCL))return [0,0,0,0];
  let v=(s.B08+s.B04)>0?(s.B08-s.B04)/(s.B08+s.B04):0;
  if(v>=0.55)return [0.08,0.50,0.23,0.85];
  if(v>=0.35)return [0.27,0.71,0.36,0.85];
  if(v>=0.20)return [0.92,0.77,0.23,0.85];
  if(v>=0.08)return [0.90,0.44,0.15,0.85];
  return [0.74,0.19,0.17,0.85];
}
"""
class ImageryRequest(BaseModel):
    month:str=Field(pattern=r'^\d{4}-(0[1-9]|1[0-2])$')
    layer:str=Field(pattern=r'^(true_color|ndvi)$')

def _copernicus_access_token():
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
      'evalscript':NDVI_IMAGE_SCRIPT if v.layer=='ndvi' else TRUE_COLOR_SCRIPT}
    try:
        token=_copernicus_access_token()
        data=_process_image(payload,token)
    except RuntimeError as ex:
        raise HTTPException(502,str(ex)) from ex
    return {'image_data_url':'data:image/png;base64,'+base64.b64encode(data).decode('ascii'),
            'bounds':bbox,'month':v.month,'capture_date':None,'period_start':period.isoformat(),'period_end':end.isoformat(),'layer':v.layer,'provider':'Copernicus Data Space Sentinel Hub Process API',
            'note':'True Sentinel-2 L2A composite for requested period; not a single dated acquisition.'}

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
                                 'resx':10,'resy':10}}
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
            count=v1.get('sampleCount',0);nodata=v1.get('noDataCount',count)
            pct=(count-nodata)/count*100 if count else 0
            vi=v1.get('mean');mi=v2.get('mean')
            if vi is None or pct<60:
                skipped+=1;continue
            dt=item['interval']['from'][:10]
            db.execute('''INSERT INTO observations(area_id,measured_at,ndvi,ndmi,valid_pixels_pct,source,is_demo,created_at)
                VALUES(?,?,?,?,?,'Copernicus Sentinel-2 L2A',0,?)
                ON CONFLICT(area_id,measured_at,source) DO UPDATE SET
                ndvi=excluded.ndvi,ndmi=excluded.ndmi,valid_pixels_pct=excluded.valid_pixels_pct''',
                (area_id,dt,round(vi,5),round(mi,5) if mi is not None else None,round(pct,1),utcnow()))
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
