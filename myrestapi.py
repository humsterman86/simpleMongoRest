import json
import ast
from bottle import route, run, request, abort, response
from json import dumps
from bson import ObjectId, json_util
import pymongo
import serverconfig as cfg

myclient = pymongo.MongoClient(cfg.mongoServer)
mydb = myclient[cfg.mongoClient]
mycol = mydb[cfg.mongoDb]


#
# Route for adding single recdord
#

@route('/stat', method='PUT')
def put_document():
    data = request.body.readline()
    if not data:
        abort(400, 'No data received')

    entity = json.loads(data)
    x = mycol.insert_one(entity)
    rv = [{ "id": str(x.inserted_id)}]
    response.content_type = 'application/json'
    return dumps(rv)

#
# Route for adding bulk records
#

@route('/statbulk', method='PUT')
def put_document():
    data = request.body.readline()
    if not data:
        abort(400, 'No data received')

    entity = json.loads(data)
    x = mycol.insert_many(entity)
    entity = x.inserted_ids
    page_sanitized = json.loads(json_util.dumps(entity))
    response.content_type = 'application/json'
    return dumps(page_sanitized)

#
# Route for getting single document
#

@route('/stat/:id', method='GET')
def get_document(id):
    entity = mycol.find_one({'_id': ObjectId(id)})
    if not entity:
        abort(404, 'No document with id %s' % id)

    page_sanitized = json.loads(json_util.dumps(entity))
    return page_sanitized

#
# Route for get list of documents with pagination and filters
# Pagination: page = X
# Limit: limit = Y
#
# Params:
# page - pagination param
# limit - documents per page. Default value is 10
#
# Filter syntax:
# $eq	Matches values that are equal to a specified value.
# $gt	Matches values that are greater than a specified value.
# $gte	Matches values that are greater than or equal to a specified value.
# $in	Matches any of the values specified in an array.
# $lt	Matches values that are less than a specified value.
# $lte	Matches values that are less than or equal to a specified value.
# $ne	Matches all values that are not equal to a specified value.
# $nin	Matches none of the values specified in an array.
#

@route('/list', method='GET')
def get_document_page():
    page = request.query.page
    limit = request.query.limit
    filters = {}

    if request.query.filters:
        filters = ast.literal_eval(request.query.filters)

    if not limit:
        limit = 10

    if not page:
        entity = mycol.find(filters).limit(int(limit))

    else:
        entity = mycol.find(filters).skip(int(page) * int(limit)).limit(int(limit))
    page_sanitized = json.loads(json_util.dumps(entity))
    response.content_type = 'application/json'
    return dumps(page_sanitized)

run(host='localhost', port=8089)