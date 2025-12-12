import os
from copy import deepcopy
from typing import Dict, Any, List, Tuple

import mcp

from fibery_mcp_server.fibery_client import FiberyClient, Schema, Database

query_tool_name = "query_database"


def query_tool() -> mcp.types.Tool:
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "descriptions", "query"), "r") as file:
        description = file.read()

    return mcp.types.Tool(
        name=query_tool_name,
        description=description,
        inputSchema={
            "type": "object",
            "properties": {
                "q_from": {
                    "type": "string",
                    "description": 'Specifies the entity type in "Space/Type" format (e.g., "Software Development/Task", "Product Management/Feature"). IMPORTANT: There is NO "database_name" parameter - use q_from to specify the database!',
                },
                "q_select": {
                    "type": "object",
                    "description": "\n".join(
                        [
                            "Defines what fields to retrieve. MUST be an object mapping aliases to field paths.",
                            "",
                            "CORRECT FORMAT:",
                            '- Primitive fields: {"AliasName": "Space/FieldName"}',
                            '  Example: {"Name": "Software Development/name", "Id": "fibery/id"}',
                            '- Related entity fields: {"AliasName": ["RelatedEntity", "field"]}',
                            '  Example: {"State": ["workflow/state", "enum/name"]}',
                            '- Sub-queries for 1-to-many: {"AliasName": {"q/from": "Type", "q/select": {...}, "q/limit": 50}}',
                            '  IMPORTANT: q/limit in sub-queries must be a NUMBER, not a string!',
                            "",
                            "COMMON MISTAKES TO AVOID:",
                            '❌ Using array format: ["Name", "Id"] - WRONG!',
                            '❌ Using boolean values: {"Name": true} - WRONG!',
                            '❌ Using SQL-style: "Name, Id" - WRONG!',
                            '✅ Correct: {"Name": "Space/name", "Id": "fibery/id"}',
                        ]
                    ),
                },
                "q_where": {
                    "type": "array",
                    "items": {},  # Allow any items in the array for flexible filter syntax
                    "description": "\n".join(
                        [
                            "CRITICAL: This parameter is called 'q_where' (NOT 'where'!). It uses ARRAY format, NOT SQL syntax!",
                            "",
                            'Filter conditions in array format [operator, [field_path], value] or ["q/and"|"q/or", ...conditions].',
                            "",
                            "CRITICAL RULES:",
                            '- ALL values in filters MUST use "$param" syntax and be defined in q_params. NEVER pass values directly!',
                            '- Field paths MUST match your database schema exactly (use "Space/FieldName" format, e.g., "Software Development/name")',
                            '- For nested fields use ["RelatedEntity", "field"] format (e.g., ["workflow/state", "enum/name"])',
                            "",
                            "Common patterns:",
                            '- Simple comparison: ["=", ["Space/FieldName"], "$param"]',
                            '- Collection membership: ["q/contains", ["Space/CollectionField", "fibery/id"], "$itemId"]',
                            '  IMPORTANT: q/contains ONLY works for checking if an item exists in a collection (many-to-many relationships)',
                            '  You MUST include "fibery/id" in the field path when using q/contains!',
                            '- Logical combinations: ["q/and", ["<", ["field1"], "$param1"], ["=", ["field2"], "$param2"]]',
                            "",
                            "Available operators: =, !=, <, <=, >, >=, q/contains (collections only), q/not-contains, q/in, q/not-in",
                            "",
                            "IMPORTANT LIMITATIONS:",
                            "- Text search (substring matching) is NOT supported. Use exact match (=) only.",
                            "- q/contains does NOT work for text search - only for collection membership checks.",
                            '- SQL syntax like "field LIKE \'%value%\'" or "field = \'value\'" will NOT work!',
                        ]
                    ),
                },
                "q_order_by": {
                    "type": "object",
                    "description": 'List of sorting criteria in format {"field1": "q/asc", "field2": "q/desc"}',
                },
                "q_limit": {
                    "type": "integer",
                    "description": "Number of results per page (defaults to 50). Maximum allowed value is 1000",
                },
                "q_offset": {
                    "type": "integer",
                    "description": "Number of results to skip. Mainly used in combination with limit and orderBy for pagination.",
                },
                "q_params": {
                    "type": "object",
                    "description": 'REQUIRED when using q_where! Dictionary of parameter values referenced in filters using "$param" syntax. ALL values in q_where MUST be passed here, never directly in the where clause. Example: {"$fromDate": "2025-01-01", "$status": "Active"}',
                },
            },
            "required": ["q_from", "q_select"],
        },
    )


def parse_q_order_by(q_order_by: Dict[str, str] | None) -> List[Tuple[List[str], str]] | None:
    if not q_order_by:
        return None
    return [([field], q_order) for field, q_order in q_order_by.items()]


def get_rich_text_fields(q_select: Dict[str, Any], database: Database) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    rich_text_fields = []
    safe_q_select = deepcopy(q_select)
    for field_alias, field_name in safe_q_select.items():
        # Skip sub-queries (dict values) - they don't need rich text processing
        if isinstance(field_name, dict):
            continue
            
        if not isinstance(field_name, str):
            if isinstance(field_name, list):
                field_name = field_name[0]
            else:
                # Skip any other non-string, non-list, non-dict types
                continue
        
        # Get the field from database schema, skip if not found
        field = database.fields_by_name().get(field_name, None)
        if field is None:
            continue
            
        if field.is_rich_text():
            rich_text_fields.append({"alias": field_alias, "name": field_name})
            safe_q_select[field_alias] = [field_name, "Collaboration~Documents/secret"]
    return rich_text_fields, safe_q_select


async def handle_query(fibery_client: FiberyClient, arguments: Dict[str, Any]) -> List[mcp.types.TextContent]:
    q_from, q_select = arguments["q_from"], arguments["q_select"]

    schema: Schema = await fibery_client.get_schema()
    database = schema.databases_by_name()[arguments["q_from"]]
    rich_text_fields, safe_q_select = get_rich_text_fields(q_select, database)

    base = {
        "q/from": q_from,
        "q/select": safe_q_select,
        "q/limit": arguments.get("q_limit", 50),
    }
    optional = {
        k: v
        for k, v in {
            "q/where": arguments.get("q_where", None),
            "q/order-by": parse_q_order_by(arguments.get("q_order_by", None)),
            "q/offset": arguments.get("q_offset", None),
        }.items()
        if v is not None
    }
    query = base | optional

    commandResult = await fibery_client.query(query, arguments.get("q_params", None))

    if not commandResult.success:
        return [mcp.types.TextContent(type="text", text=str(commandResult))]

    for i, entity in enumerate(commandResult.result):
        for field in rich_text_fields:
            secret = entity.get(field["alias"], None)
            if not secret:
                return [
                    mcp.types.TextContent(
                        type="text", text=f"Unable to get document content for entity {entity}. Field: {field}"
                    )
                ]
            entity[field["alias"]] = await fibery_client.get_document_content(secret)
    return [mcp.types.TextContent(type="text", text=str(commandResult))]
