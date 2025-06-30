import singer
from singer import metrics, metadata, Transformer
from singer.bookmarks import set_currently_syncing

from tap_outreach.client import InvalidCredentialsError, UserNotActiveError
from tap_outreach.discover import discover

LOGGER = singer.get_logger()

STEAM_CONFIGS = {
    "accounts": {
        "url_path": "accounts",
        "replication": "incremental",
        "filter_field": "updatedAt",
        "fks": ["creatorId", "ownerId", "updaterId"],
    },
    "calls": {
        "url_path": "calls",
        "replication": "incremental",
        "filter_field": "updatedAt",
        "fks": [
            "callDispositionId",
            "callPurposeId",
            "opportunityId",
            "prospectId",
            "sequenceId",
            "sequenceStateId",
            "sequenceStepId",
            "taskId",
            "userId",
        ],
    },
    "events": {
        "url_path": "events",
        "replication": "incremental",
        "filter_field": "eventAt",
        "fks": ["prospectId", "userId"],
    },
    "mailboxes": {
        "url_path": "mailboxes",
        "replication": "incremental",
        "filter_field": "updatedAt",
        "fks": ["creatorId", "updaterId"],
    },
    "mailings": {
        "url_path": "mailings",
        "replication": "incremental",
        "filter_field": "updatedAt",
        "fks": [
            "calendarId",
            "mailboxId",
            "opportunityId",
            "prospectId",
            "sequenceId",
            "sequenceStateId",
            "sequenceStepId",
            "taskId",
            "templateId",
        ],
    },
    "opportunities": {
        "url_path": "opportunities",
        "replication": "incremental",
        "filter_field": "updatedAt",
        "fks": ["accountId", "creatorId", "opportunityStageId", "ownerId"],
    },
    "personas": {
        "url_path": "personas",
        "replication": "incremental",
        "filter_field": "updatedAt",
    },
    "prospects": {
        "url_path": "prospects",
        "replication": "incremental",
        "filter_field": "updatedAt",
        "fks": [
            "accountId",
            "creatorId",
            "defaultPluginMappingId",
            "ownerId",
            "personaId",
            "stageId",
            "updaterId",
        ],
    },
    "stages": {
        "url_path": "stages",
        "replication": "incremental",
        "filter_field": "updatedAt",
        "fks": ["creatorId", "updaterId"],
    },
    "users": {
        "url_path": "users",
        "replication": "incremental",
        "filter_field": "updatedAt",
        "fks": [
            "calendarId",
            "mailboxId",
            "profileId",
            "roleId",
            "creatorId",
            "updaterId",
        ],
    },
    "sequences": {
        "url_path": "sequences",
        "replication": "incremental",
        "filter_field": "updatedAt",
        "fks": ["creatorId", "ownerid", "updaterId"],
    },
    "sequence_states": {
        "url_path": "sequenceStates",
        "replication": "incremental",
        "filter_field": "updatedAt",
        "fks": ["accountid", "creatorId", "prospectId", "sequenceId"],
    },
    "sequence_steps": {
        "url_path": "sequenceSteps",
        "replication": "incremental",
        "filter_field": "updatedAt",
        "fks": ["creatorId", "sequenceId", "updaterId"],
    },
    "sequence_templates": {
        "url_path": "sequenceTemplates",
        "replication": "full",
        "filter_field": "updatedAt",
        "fks": ["creatorId", "updaterId"],
    },
}


def get_bookmark(state, stream_name, default):
    return state.get("bookmarks", {}).get(stream_name, default)


def write_bookmark(state, stream_name, value):
    if "bookmarks" not in state:
        state["bookmarks"] = {}
    state["bookmarks"][stream_name] = value
    singer.write_state(state)


def write_schema(stream):
    schema = stream.schema.to_dict()
    singer.write_schema(stream.tap_stream_id, schema, stream.key_properties)


def process_records(stream, mdata, max_modified, records, filter_field, fks):
    schema = stream.schema.to_dict()
    with metrics.record_counter(stream.tap_stream_id) as counter:
        for record in records:
            attributes = record["attributes"]
            if filter_field in attributes and attributes[filter_field] > max_modified:
                max_modified = attributes[filter_field]
            singer.write_record(stream.tap_stream_id, record)
            counter.increment()
        return max_modified


def sync_endpoint(client, catalog, state, start_date, stream, mdata):
    stream_name = stream.tap_stream_id
    last_datetime = get_bookmark(state, stream_name, start_date)

    write_schema(stream)

    stream_config = STEAM_CONFIGS[stream_name]
    filter_field = stream_config.get("filter_field")
    fks = stream_config.get("fks", [])

    count = 250
    offset = 0
    has_more = True
    max_modified = last_datetime
    paginate_datetime = last_datetime
    LOGGER.info(
        "{} - Syncing data since {}".format(stream.tap_stream_id, last_datetime)
    )
    while has_more:
        query_params = {"page[limit]": count, "page[offset]": offset}

        if stream_config.get("replication") == "incremental":
            query_params["filter[{}]".format(filter_field)] = "{}..inf".format(
                paginate_datetime
            )
            query_params["sort"] = filter_field

        data = client.get(
            stream_config["url_path"], params=query_params, endpoint=stream_name
        )
        records = data["data"]

        if len(records) < count:
            has_more = False
        else:
            offset += count

        max_modified = process_records(
            stream, mdata, max_modified, records, filter_field, fks
        )

        if offset > 10000:
            paginate_datetime = max_modified
            offset = 0

        if stream_config.get("replication") == "incremental":
            write_bookmark(state, stream_name, max_modified)


def update_current_stream(state, stream_name=None):
    singer.write_state(state)


def sync(client, catalog, state, start_date):
    if not catalog:
        catalog = discover()
        selected_streams = catalog.streams
    else:
        selected_streams = catalog.get_selected_streams(state)

    selected_streams = sorted(selected_streams, key=lambda x: x.tap_stream_id)

    for stream in selected_streams:
        try:
            if stream.tap_stream_id in [
                "accounts",
                "prospects",
                "calls",
                "users",
                "personas",
                "mailings",
                "sequences",
            ]:
                mdata = metadata.to_map(stream.metadata)
                update_current_stream(state, stream.tap_stream_id)
                sync_endpoint(client, catalog, state, start_date, stream, mdata)
        except KeyboardInterrupt:
            raise
        except InvalidCredentialsError:
            update_current_stream(state)
            raise
        except UserNotActiveError:
            update_current_stream(state)
            raise
        except Exception as ex:
            LOGGER.exception("an unknown error occured", exc_info=ex)
            update_current_stream(state)
            continue

    update_current_stream(state)
