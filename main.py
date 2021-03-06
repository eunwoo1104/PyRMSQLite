import flask
import flask_restful
import sqlite3
import time
import threading

app = flask.Flask(__name__)
app.config['JSON_AS_ASCII'] = False
api = flask_restful.Api(app)
clients_dict = {}


def check_if_req_key_exist(req_keys, json_data):
    forgot = [x for x in req_keys if x not in json_data.keys()]
    why_none = [x for x in json_data.keys() if x in req_keys and json_data[x] is None and x != "params"]
    if len(forgot) != 0:
        return flask_restful.abort(400, description=f"Wrong json format. You forgot: {', '.join(forgot)}")
    if len(why_none) != 0:
        return flask_restful.abort(400, description=f"Values of required keys are None(null). Keys with None(null) value: {', '.join(why_none)}")
    return None


def check_if_logged_in(ip, db_name):
    if ip not in clients_dict.keys():
        return flask_restful.abort(400, description="Please login first! Note: Login expires per 15 min.")
    count = 0
    for x in clients_dict[ip]:
        if db_name == x["db"]:
            count += 1
    if count == 0:
        return flask_restful.abort(400, description="Requested unauthorized DB.")
    return None


def get_login_info(login_id, pw):
    login_db = sqlite3.connect("login.db")
    login_db.row_factory = sqlite3.Row
    cur = login_db.cursor()
    try:
        cur.execute("SELECT * FROM login WHERE login_id = ? and pw = ?", (login_id, pw))
        res = dict(cur.fetchone())
        if not len(res):
            return False
        allowed_db = (res["allowed_db"]).split(', ') if res["allowed_db"] is not None else None
        return dict(login_id=res["login_id"], pw=res["pw"], allowed_db=allowed_db)
    finally:
        cur.close()
        login_db.close()


def process_sql(ip, json_data):
    sqlite_db = sqlite3.connect("db/" + clients_dict[ip][0]["id"] + "_" + json_data["db_name"] + ".db")
    sqlite_db.row_factory = sqlite3.Row
    cur = sqlite_db.cursor()
    try:
        if json_data["res_required"] is True:
            if json_data["params"] is None:
                cur.execute(json_data["expression"])
            else:
                cur.execute(json_data["expression"], json_data["params"])
            return {"result": [dict(x) for x in cur.fetchall()]}
        if json_data["params"] is None:
            sqlite_db.execute(json_data["expression"])
        else:
            sqlite_db.execute(json_data["expression"], json_data["params"])
        sqlite_db.commit()
    finally:
        cur.close()
        sqlite_db.close()


def check_if_session_expired():
    while True:
        del_tgt = []
        exp_tgt = []
        time_now = time.time()
        if bool(clients_dict):
            for a in clients_dict.keys():
                if len(clients_dict[a]) != 0:
                    for b in clients_dict[a]:
                        logged_in_time = b["login_time"]
                        if (time_now - logged_in_time) > 60 * 1:
                            del_tgt.append((a, b))
                            break
                elif len(clients_dict[a]) == 0:
                    exp_tgt.append(a)
            for a, b in del_tgt:
                clients_dict[a].remove(b)
            if len(exp_tgt) != 0:
                for c in exp_tgt:
                    del clients_dict[c]
        time.sleep(1)


class LoginSQLiteAPI(flask_restful.Resource):
    # noinspection PyMethodMayBeStatic
    def post(self):
        ip = flask.request.remote_addr
        req_keys = ["id", "pw", "db"]
        json_data = flask.request.get_json(force=True)
        res = check_if_req_key_exist(req_keys, json_data)
        if res is not None:
            return res
        login_data = get_login_info(json_data["id"], json_data["pw"])
        if login_data is False:
            return flask_restful.abort(400, description="Login failed. Did you send correct id and pw?")
        login_info = dict(id=json_data["id"], pw=json_data["pw"], db=json_data["db"], login_time=time.time())
        try:
            clients_dict[ip].append(login_info)
        except KeyError:
            clients_dict[ip] = [login_info]
        print(clients_dict[ip])
        return f"Successfully logged in as {json_data['id']} with {json_data['db']}."


class SQLiteAPI(flask_restful.Resource):
    # noinspection PyMethodMayBeStatic
    def post(self):
        ip = flask.request.remote_addr
        req_keys = ["db_name", "res_required", "expression", "params"]
        json_data = flask.request.get_json(force=True)
        res = check_if_req_key_exist(req_keys, json_data)
        if res is not None:
            return res
        is_logged_in = check_if_logged_in(ip, json_data["db_name"])
        if is_logged_in is not None:
            return is_logged_in
        return process_sql(ip, json_data)


class AdminSys(flask_restful.Resource):
    # noinspection PyMethodMayBeStatic
    def post(self):
        ip = flask.request.remote_addr
        req_keys = ["id", "pw", "action", "script"]
        if ip != "172.0.0.1":
            return flask_restful.abort(403, description="Unauthorized IP.")
        json_data = flask.request.get_json(force=True)
        res = check_if_req_key_exist(req_keys, json_data)


api_dict = dict(sqlite_db=SQLiteAPI, login=LoginSQLiteAPI)
for k, v in api_dict.items():
    if str(k) == "front":
        k = ""
    api.add_resource(v, '/' + str(k))
t = threading.Thread(target=check_if_session_expired)
t.start()
app.run(debug=True)
