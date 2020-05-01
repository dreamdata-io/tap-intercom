# tap-intercom

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from the [Intercom v2.0 API](https://developers.intercom.com/intercom-api-reference/reference#introduction)
- Extracts the following resources:
  - [Admins](https://developers.intercom.com/intercom-api-reference/reference#list-admins)
  - [Companies](https://developers.intercom.com/intercom-api-reference/reference#list-companies)
  - [Conversations](https://developers.intercom.com/intercom-api-reference/reference#list-conversations)
    - [Conversation Parts](https://developers.intercom.com/intercom-api-reference/reference#get-a-single-conversation)
  - [Data Attributes](https://developers.intercom.com/intercom-api-reference/reference#list-data-attributes)
  - [Segments](https://developers.intercom.com/intercom-api-reference/reference#list-segments)
  - [Tags](https://developers.intercom.com/intercom-api-reference/reference#list-tags-for-an-app)
  - [Teams](https://developers.intercom.com/intercom-api-reference/reference#list-teams)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state


## Streams

[admins](https://developers.intercom.com/intercom-api-reference/reference#list-admins)
- Endpoint: https://api.intercom.io/admins
- Primary key fields: id
- Foreign key fields: team_ids
- Replication strategy: FULL_TABLE
- Transformations: none

[companies](https://developers.intercom.com/intercom-api-reference/reference#list-companies)
- Endpoint: https://api.intercom.io/companies
- Primary key fields: id
- Foreign key fields: segments > id, tags > id
- Replication strategy: INCREMENTAL (query all, filter results)
  - Bookmark: updated_at (date-time)
- Transformations: de-nest segments, tags

[conversations](https://developers.intercom.com/intercom-api-reference/reference#list-conversations)
- Endpoint: https://api.intercom.io/conversations
- Primary key fields: id
- Foreign key fields: assignee > id, author > id, customer > id, customers > id, teammate > id, tags > id, user > id
- Replication strategy: INCREMENTAL (query all, filter results)
  - Sort: updated_at asc
  - Bookmark: updated_at (date-time)
- Transformations: de-nest customers, tags

[conversation_parts](https://developers.intercom.com/intercom-api-reference/reference#get-a-single-conversation)
- Endpoint: https://api.intercom.io/conversations/{conversation_id}
- Primary key fields: id
- Foreign key fields: conversation_id, author > id
- Replication strategy: FULL_TABLE (ALL for each changed parent Conversation)
- Transformations: Conversation parts with parent conversation_id

[segments](https://developers.intercom.com/intercom-api-reference/reference#list-segments)
- Endpoint: https://api.intercom.io/segments
- Primary key fields: id
- Foreign key fields: none
- Replication strategy: INCREMENTAL (query all, filter results)
  - Bookmark: updated_at (date-time)
- Transformations: none

[tags](https://developers.intercom.com/intercom-api-reference/reference#list-tags-for-an-app)
- Endpoint: https://api.intercom.io/tags
- Primary key fields: id
- Foreign key fields: none
- Replication strategy: FULL_TABLE
- Transformations: none

[teams](https://developers.intercom.com/intercom-api-reference/reference#list-teams)
- Endpoint: https://api.intercom.io/teams
- Primary key fields: id
- Foreign key fields: admin_ids
- Replication strategy: FULL_TABLE
- Transformations: none

## Authentication

## Quick Start

1. Install

    Clone this repository, and then install using setup.py. We recommend using a virtualenv:

    ```bash
    > virtualenv -p python3 venv
    > source venv/bin/activate
    > python setup.py install
    OR
    > cd .../tap-intercom
    > pip install .
    ```
2. Dependent libraries
    The following dependent libraries were installed.
    ```bash
    > pip install singer-python
    > pip install singer-tools
    > pip install target-stitch
    > pip install target-json
    
    ```
    - [singer-tools](https://github.com/singer-io/singer-tools)
    - [target-stitch](https://github.com/singer-io/target-stitch)

3. Create your tap's `config.json` file. Intercom [Authentication Types](https://developers.intercom.com/building-apps/docs/authentication-types) explains how to get an `access_token`. Make sure your [OAuth Scope](https://developers.intercom.com/building-apps/docs/oauth-scopes) allows Read access to the endpoints above. Additionally, your App should use [API Version ](https://developers.intercom.com/building-apps/docs/update-your-api-version) **[v1.4](https://developers.intercom.com/intercom-api-reference/v1.4/reference)**.

    ```json
    {
        "access_token": "YOUR_API_ACCESS_TOKEN",
        "start_date": "2019-01-01T00:00:00Z",
    }
    ```
    
    Optionally, also create a `state.json` file. `currently_syncing` is an optional attribute used for identifying the last object to be synced in case the job is interrupted mid-stream. The next run would begin where the last job left off.

    ```json
    {
        "bookmarks": {
          "companies": {
            "updated_at": "2020-04-30T14:37:41+00:00"
          },
          "contacts": {
            "updated_at": "2020-04-30T16:21:56+00:00"
          },
          "conversations": {
            "updated_at": "2020-04-30T09:38:38+00:00"
          },
          "segments": {
            "updated_at": "2020-02-20T12:31:38+00:00"
          }
        }
    }
    ```

4. Run the Tap in Discovery Mode
    This creates a catalog.json for selecting objects/fields to integrate:
    ```bash
    tap-intercom --config config.json --discover > catalog.json
    ```
   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#discovery-mode).

5. Run the Tap in Sync Mode (with catalog) and [write out to state file](https://github.com/singer-io/getting-started/blob/master/docs/RUNNING_AND_DEVELOPING.md#running-a-singer-tap-with-a-singer-target)

    For Sync mode:
    ```bash
    > tap-intercom --config tap_config.json --catalog catalog.json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To load to json files to verify outputs:
    ```bash
    > tap-intercom --config tap_config.json --catalog catalog.json | target-json > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```
    To pseudo-load to [Stitch Import API](https://github.com/singer-io/target-stitch) with dry run:
    ```bash
    > tap-intercom --config tap_config.json --catalog catalog.json | target-stitch --config target_config.json --dry-run > state.json
    > tail -1 state.json > state.json.tmp && mv state.json.tmp state.json
    ```

