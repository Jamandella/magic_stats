from flask import Blueprint, jsonify
import backend.stataccess


# Create API Blueprint with /api prefix
api_bp = Blueprint('api', __name__, url_prefix='/api')


# Helper function - move to a separate file
def parse_bool(val: str) -> bool:
    return str(val).strip().lower() in ("true", "1", "yes")


@api_bp.route('/')
def Welcome():
    return jsonify({"message": "Welcome to the API!!!"})


@api_bp.route('/getActiveSets', methods=['GET'])
def ActiveSets():
    return backend.stataccess.getActiveSets()


@api_bp.route('/getCardInfo/<set_abbr>', methods=['GET'])
def CardInfo(set_abbr: str):
    json_card = backend.stataccess.getCardInfo(set_abbr)
    return json_card


@api_bp.route('/getArchAvgCurve/<set_abbr>/<arch_label>', methods=['GET'])
def ArchAvgCurve(set_abbr: str, arch_label: str):
    json_card = backend.stataccess.getArchAvgCurve(set_abbr, arch_label)
    return json_card


@api_bp.route('/getArchRecord/<set_abbr>/<arch_label>', methods=['GET'])
def ArchRecords(set_abbr: str, arch_label: str):
    json_card = backend.stataccess.getArchRecord(set_abbr, arch_label)
    return json_card


# works but if all params not provided just returns the home page
# if as jason is "false" returns server error
@api_bp.route('/getCardInDeckWinRates/<set_abbr>/<arch_labels>/<int:min_copies>/<int:max_copies>/<index_by_name>/<as_json>', methods=['GET'])
def CardInDeckWinRates(
    set_abbr: str, 
    arch_labels: str, 
    min_copies: int, 
    max_copies: int, 
    index_by_name: str, 
    as_json: str
):
    json_card = backend.stataccess.getCardInDeckWinRates(
        set_abbr, 
        arch_labels, 
        min_copies, # converted to int by flask
        max_copies, # converted to int by flask
        index_by_name, 
        parse_bool(as_json)
    )
    return json_card
