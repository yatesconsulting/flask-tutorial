from flask import (
    Blueprint, flash, g, redirect, render_template, request, url_for, send_file, session
    
)
from werkzeug.exceptions import abort

from flaskr.auth import login_required
from flaskr.db import get_db
# from flask.helpers import read_image, make_response

import io

bp = Blueprint('logos', __name__, url_prefix='/logos')

# VLAN	Logo
# 61	https://www.risd.k12.nm.us/UserFiles/Servers/Server_18731897/Image/Creative%20Learning%20Center/AC%20Logo_from%20lesson%20plan.png
# 36	https://www.risd.k12.nm.us/UserFiles/Servers/Server_18734232/Image/eceBearLogoTrans.png
# 23	https://www.risd.k12.nm.us/UserFiles/Servers/Server_18735288/Templates/images/MVMS-logo.png
# 99	https://www.risd.k12.nm.us/UserFiles/Servers/Server_18731897/Image/Banner/RISD_black_logoNotTransparent.png
# 42	https://www.risd.k12.nm.us/UserFiles/Servers/Server_18735771/Templates/images/PES-logo.png
# 34	https://www.risd.k12.nm.us/UserFiles/Servers/Server_18732762/Templates/images/EGPES-logo.png
# 40	https://www.risd.k12.nm.us/UserFiles/Servers/Server_18735447/Templates/images/NLES-logo.png
# 24	https://www.risd.k12.nm.us/UserFiles/Servers/Server_18736098/Templates/images/SMS-logo.png
# 43	https://sun.risd.k12.nm.us/UserFiles/Servers/Server_18736264/Image/PumaMFwMaskSunflower.gif
# 39	https://www.risd.k12.nm.us/UserFiles/Servers/Server_18735131/Templates/images/MES-logo.png
# 31	https://www.risd.k12.nm.us/UserFiles/Servers/Server_18732070/Templates/images/BES-logo.png
# 37	https://www.risd.k12.nm.us/UserFiles/Servers/Server_18734813/Templates/images/MHES-logo.png
# 45	https://www.risd.k12.nm.us/UserFiles/Servers/Server_18736737/Templates/images/WAES-logo.png
# 33	https://www.risd.k12.nm.us/UserFiles/Servers/Server_18732404//Header/header_logo.gif
# 21	https://www.risd.k12.nm.us/UserFiles/Servers/Server_18732238/Templates/images/BMS-logo.png
# 11	https://www.risd.k12.nm.us/UserFiles/Servers/Server_18734505/Templates/images/GHS-logo.png
# 12	https://www.risd.k12.nm.us/UserFiles/Servers/Server_18735929/Templates/images/RHS-logo.png
# 13	https://www.risd.k12.nm.us/UserFiles/Servers/Server_18736416/Templates/images/UHS-logo.png
# 38	https://www.risd.k12.nm.us/UserFiles/Servers/Server_18734967/Templates/images/MAES-logo.png
# 41	https://www.risd.k12.nm.us/UserFiles/Servers/Server_18735601/Templates/images/PELC-logo.png
# 44	https://www.risd.k12.nm.us/UserFiles/Servers/Server_18736579/Templates/images/VVES-logo.png
	

@bp.route('/')
def index():
    pid = "99.png" # for website
    image_binary = read_image(pid)
    response = make_response(image_binary)
    response.headers.set('Content-Type', 'image/jpeg')
    response.headers.set(
        'Content-Disposition', 'attachment', filename='%s.jpg' % pid)
    return response

    return send_file(
        io.BytesIO(image_binary),
        mimetype='image/png',
        as_attachment=True,
        attachment_filename='%s.jpg' % pid)