
#app.py
from flask import Flask, jsonify, redirect, url_for, session
import sqlite3, os, math
from datetime import datetime, timedelta
import requests

# Google OAuth imports
from flask import request
from google.oauth2 import id_token
from google_auth_oauthlib.flow import Flow
import pathlib

# ----------------------------
# Config
# ----------------------------
DB_PATH = "ad_attribution.db"
app = Flask(__name__)
app.secret_key = "REPLACE_WITH_RANDOM_SECRET_KEY"

# Google OAuth config
GOOGLE_CLIENT_SECRETS_FILE = "client_secret.json"  # download from Google Cloud
SCOPES = ["openid", "https://www.googleapis.com/auth/userinfo.email", "https://www.googleapis.com/auth/userinfo.profile"]
REDIRECT_URI = "http://127.0.0.1:5000/callback"

# ----------------------------
# DB helpers
# ----------------------------
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    if os.path.exists(DB_PATH):
        return
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE campaigns (
            campaign_id TEXT PRIMARY KEY,
            name TEXT,
            start_date TEXT,
            end_date TEXT,
            platforms TEXT,
            tv_regions TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            campaign_id TEXT,
            timestamp TEXT,
            event_type TEXT,
            source TEXT,
            referrer TEXT,
            geo TEXT,
            user_id TEXT,
            revenue REAL
        )
    """)
    cur.execute("""
        CREATE TABLE airings (
            airing_id INTEGER PRIMARY KEY AUTOINCREMENT,
            campaign_id TEXT,
            airing_time TEXT,
            channel TEXT,
            region TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE attribution_results (
            result_id INTEGER PRIMARY KEY AUTOINCREMENT,
            campaign_id TEXT,
            window_start TEXT,
            window_end TEXT,
            total_conversions INTEGER,
            measured_conversions INTEGER,
            unattributed INTEGER,
            inferred_tv REAL,
            confidence REAL,
            computed_at TEXT
        )
    """)
    conn.commit()
    conn.close()

# ----------------------------
# Sample data seeding
# ----------------------------
def seed_sample_data():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("INSERT OR REPLACE INTO campaigns (campaign_id,name,start_date,end_date,platforms,tv_regions) VALUES (?, ?, ?, ?, ?, ?)",
                ('camp_001','Diwali Promo','2025-11-01','2025-11-07','instagram,whatsapp,email,calls,tv','Kolkata,Delhi'))
    cur.execute("DELETE FROM events WHERE campaign_id = ?", ('camp_001',))
    cur.execute("DELETE FROM airings WHERE campaign_id = ?", ('camp_001',))
    conn.commit()

    now = datetime.utcnow()
    def ts(h): return (now + timedelta(hours=h)).isoformat()
    # 40 Instagram
    for i in range(40):
        cur.execute("INSERT INTO events (campaign_id,timestamp,event_type,source,referrer,geo,user_id,revenue) VALUES (?,?,?,?,?,?,?,?)",
                    ('camp_001', ts(i), 'conversion', 'instagram', 'instagram.com', 'Kolkata', f'inst_{i}', 100.0))
    # 30 WhatsApp
    for i in range(30):
        cur.execute("INSERT INTO events (campaign_id,timestamp,event_type,source,referrer,geo,user_id,revenue) VALUES (?,?,?,?,?,?,?,?)",
                    ('camp_001', ts(i+1), 'conversion', 'whatsapp', None, 'Kolkata', f'wa_{i}', 150.0))
    # 10 Email
    for i in range(10):
        cur.execute("INSERT INTO events (campaign_id,timestamp,event_type,source,referrer,geo,user_id,revenue) VALUES (?,?,?,?,?,?,?,?)",
                    ('camp_001', ts(i+2), 'conversion', 'email', 'campaign_email', 'Kolkata', f'email_{i}', 200.0))
    # 10 Calls
    for i in range(10):
        cur.execute("INSERT INTO events (campaign_id,timestamp,event_type,source,referrer,geo,user_id,revenue) VALUES (?,?,?,?,?,?,?,?)",
                    ('camp_001', ts(i+3), 'conversion', 'call', None, 'Kolkata', f'call_{i}', 250.0))
    # 10 unattributed (direct)
    for i in range(7):
        cur.execute("INSERT INTO events (campaign_id,timestamp,event_type,source,referrer,geo,user_id,revenue) VALUES (?,?,?,?,?,?,?,?)",
                    ('camp_001', ts(24+i), 'conversion', 'direct', None, 'Kolkata', f'direct_{i}', 180.0))
    for i in range(3):
        cur.execute("INSERT INTO events (campaign_id,timestamp,event_type,source,referrer,geo,user_id,revenue) VALUES (?,?,?,?,?,?,?,?)",
                    ('camp_001', ts(100+i), 'conversion', 'direct', None, 'Mumbai', f'direct2_{i}', 180.0))
    # TV airings
    cur.execute("INSERT INTO airings (campaign_id,airing_time,channel,region) VALUES (?,?,?,?)",
                ('camp_001', ts(23),'ZeeTV','Kolkata'))
    cur.execute("INSERT INTO airings (campaign_id,airing_time,channel,region) VALUES (?,?,?,?)",
                ('camp_001', ts(47),'ZeeTV','Kolkata'))
    conn.commit()
    conn.close()
    print("Seeded sample campaign 'camp_001'")

# ----------------------------
# Scoring / inference
# ----------------------------
def parse_iso(ts_str):
    try: return datetime.fromisoformat(ts_str)
    except: return None

def compute_scores(campaign_id, window_hours=24):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) as cnt FROM events WHERE campaign_id=? AND event_type='conversion'",(campaign_id,))
    total = cur.fetchone()['cnt']
    tracked_sources=('instagram','whatsapp','email','call','promo','qr','app')
    cur.execute(f"SELECT COUNT(*) as cnt FROM events WHERE campaign_id=? AND event_type='conversion' AND LOWER(source) IN ({','.join(['?']*len(tracked_sources))})",
                tuple([campaign_id]+list(tracked_sources)))
    measured=cur.fetchone()['cnt']
    unattributed = total - measured
    if unattributed <=0:
        return {'campaign_id':campaign_id,'total':total,'measured':measured,'unattributed':0,'inferred_tv':0.0,'confidence':1.0}
    cur.execute("SELECT * FROM events WHERE campaign_id=? AND event_type='conversion' AND (source IS NULL OR LOWER(source)='direct' OR LOWER(source)='unknown')",(campaign_id,))
    unat_events = cur.fetchall()
    unat_count = len(unat_events)
    cur.execute("SELECT * FROM airings WHERE campaign_id=?",(campaign_id,))
    airings = cur.fetchall()
    airing_times = [parse_iso(a['airing_time']) for a in airings if parse_iso(a['airing_time'])]
    airing_regions = [a['region'] for a in airings]
    # time score
    count_in_window = 0
    for e in unat_events:
        e_ts = parse_iso(e['timestamp'])
        if not e_ts: continue
        for at in airing_times:
            delta = e_ts - at
            if timedelta(0) <= delta <= timedelta(hours=window_hours):
                count_in_window +=1
                break
    score_T = count_in_window/max(1,unat_count)
    # geo
    count_geo=0
    for e in unat_events:
        geo=e['geo']
        if geo and geo in airing_regions: count_geo+=1
    score_G = count_geo/max(1,unat_count)
    # direct
    count_direct=0
    for e in unat_events:
        if not e['referrer']: count_direct+=1
    score_D=count_direct/max(1,unat_count)
    # promo
    cur.execute("SELECT COUNT(*) as cnt FROM events WHERE campaign_id=? AND event_type='conversion' AND (LOWER(source)='promo' OR LOWER(referrer) LIKE ?)",(campaign_id,'%tv_code%'))
    score_P=min(1.0, cur.fetchone()['cnt']/max(1,unat_count))
    score_U=0.0
    weights={'t':0.30,'g':0.20,'d':0.15,'p':0.25,'u':0.10}
    if score_U==0:
        total_w=sum([weights['t'],weights['g'],weights['d'],weights['p']])
        weights['t']/=total_w; weights['g']/=total_w; weights['d']/=total_w; weights['p']/=total_w; weights['u']=0.0
    S_overall=weights['t']*score_T+weights['g']*score_G+weights['d']*score_D+weights['p']*score_P+weights.get('u',0)*score_U
    inferred_tv = unattributed * S_overall
    sample_size=unat_count+measured
    confidence_factor=min(1.0, math.log10(sample_size+1)/3.0)
    confidence = S_overall * confidence_factor
    return {
        'campaign_id':campaign_id,'total':total,'measured':measured,'unattributed':unattributed,
        'scores':{'time_score':round(score_T,3),'geo_score':round(score_G,3),'direct_score':round(score_D,3),'promo_score':round(score_P,3),'uplift_score':round(score_U,3)},
        'weights':{k:round(v,3) for k,v in weights.items()},
        'S_overall':round(S_overall,3),
        'inferred_tv':round(inferred_tv,3),
        'confidence':round(confidence,3)
    }

def save_attribution_result(campaign_id,result,window_start,window_end):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("INSERT INTO attribution_results (campaign_id,window_start,window_end,total_conversions,measured_conversions,unattributed,inferred_tv,confidence,computed_at) VALUES (?,?,?,?,?,?,?,?,?)",
                (campaign_id,window_start,window_end,result['total'],result['measured'],result['unattributed'],result['inferred_tv'],result['confidence'],datetime.utcnow().isoformat()))
    conn.commit()
    conn.close()

# ----------------------------
# Alerts (mocked)
# ----------------------------
def send_email_alert(to_email, subject, body):
    print(f"[EMAIL ALERT] To:{to_email} Subject:{subject} Body:{body}")

def send_whatsapp_alert(to_number, message):
    print(f"[WHATSAPP ALERT] To:{to_number} Msg:{message}")

# ----------------------------
# Flask routes
# ----------------------------
@app.route('/seed_sample')
def seed_route():
    init_db()
    seed_sample_data()
    return jsonify({'status':'seeded sample campaign camp_001'})

@app.route('/compute/<campaign_id>')
def compute_route(campaign_id):
    init_db()
    result = compute_scores(campaign_id,window_hours=24)
    conn=get_conn()
    cur=conn.cursor()
    cur.execute("SELECT start_date,end_date FROM campaigns WHERE campaign_id=?",(campaign_id,))
    row=cur.fetchone()
    window_start=row['start_date'] if row else (datetime.utcnow()-timedelta(days=7)).isoformat()
    window_end=row['end_date'] if row else datetime.utcnow().isoformat()
    save_attribution_result(campaign_id,result,window_start,window_end)
    # send alerts
    send_email_alert("client@example.com","Campaign Update", f"Inferred TV conversions: {result['inferred_tv']} (confidence {result['confidence']:.2f})")
    send_whatsapp_alert("+910000000000", f"Inferred TV: {result['inferred_tv']}")
    return jsonify(result)
