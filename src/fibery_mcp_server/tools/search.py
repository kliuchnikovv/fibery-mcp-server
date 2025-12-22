import os
from typing import Dict, Any, List

import mcp

from fibery_mcp_server.fibery_client import FiberyClient

search_tool_name = "search_entities"


def search_tool() -> mcp.types.Tool:
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "descriptions", "search"), "r") as file:
        description = file.read()

    return mcp.types.Tool(
        name=search_tool_name,
        description=description,
        inputSchema={
            "type": "object",
            "properties": {
                "database": {
                    "type": "string",
                    "description": 'Database name in "Space/Type" format (e.g., "Software Development/Task")',
                },
                "query": {
                    "type": "string",
                    "description": "Text to search for (case-insensitive substring matching)",
                },
                "search_fields": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": 'List of field names to search in. Default: ["Name"]. Must use full field names like "Software Development/name"',
                },
                "return_fields": {
                    "type": "object",
                    "description": 'Fields to return in results. Same format as q_select in query_database. Default: {"Name": "Space/name", "Id": "fibery/id"}',
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of entities to scan in this batch for filtering. Default: 500.",
                },
                "offset": {
                    "type": "integer",
                    "description": "Number of entities to skip before starting the scan (for pagination). Default: 0.",
                },
            },
            "required": ["database", "query"],
        },
    )


async def handle_search(fibery_client: FiberyClient, arguments: Dict[str, Any]) -> List[mcp.types.TextContent]:
    database = arguments["database"]
    query = arguments["query"].lower()  # Case-insensitive search
    limit = arguments.get("limit", 500)
    offset = arguments.get("offset", 0)
    
    # Determine search fields
    search_fields = arguments.get("search_fields")
    if not search_fields:
        # Default: search in Name field
        # Extract space from database name (e.g., "Software Development/Task" -> "Software Development")
        space = "/".join(database.split("/")[:-1])
        search_fields = [f"{space}/name"]
    
    # Determine return fields
    return_fields = arguments.get("return_fields")
    if not return_fields:
        # Default return fields
        space = "/".join(database.split("/")[:-1])
        return_fields = {
            "Name": f"{space}/name",
            "Id": "fibery/id"
        }
    
    # Build q_select: combine search_fields and return_fields
    q_select = {}
    
    # Add return fields
    for alias, field_spec in return_fields.items():
        q_select[alias] = field_spec
    
    # Add search fields if not already in return fields
    for search_field in search_fields:
        # Create alias from field name
        field_alias = f"_search_{search_field.replace('/', '_').replace(' ', '_')}"
        if search_field not in return_fields.values():
            q_select[field_alias] = search_field
    
    # Query database
    query_result = await fibery_client.query(
        {
            "q/from": database,
            "q/select": q_select,
            "q/limit": limit,
            "q/offset": offset,
            "q/order": ["fibery/creation-date", "desc"],
        },
        None
    )
    
    if not query_result.success:
        return [mcp.types.TextContent(type="text", text=f"Error querying database: {query_result.result}")]
    
    entities = query_result.result
    matching_entities = []
    
    # Filter results client-side
    for entity in entities:
        # Check if query appears in any search field
        match_found = False
        for search_field in search_fields:
            field_value = None
            
            # Find the value in the entity (check fields and aliases)
            field_alias = f"_search_{search_field.replace('/', '_').replace(' ', '_')}"
            
            # Try finding value by alias first, then by raw field name
            if field_alias in entity:
                field_value = entity[field_alias]
            else:
                # Check if this search field maps to a returned alias
                # Iterate through q_select to see if any alias maps to this search_field
                found_alias = None
                for q_alias, q_field in q_select.items():
                    if q_field == search_field:
                        found_alias = q_alias
                        break
                
                if found_alias and found_alias in entity:
                    field_value = entity[found_alias]
                else:
                    # Fallback: Look for value matching the field path/name directly
                    for key, value in entity.items():
                        if key == search_field:
                            field_value = value
                            break

            if field_value and isinstance(field_value, str) and query in field_value.lower():
                match_found = True
                break
        
        if match_found:
            # Remove internal search fields from result
            filtered_entity = {k: v for k, v in entity.items() if not k.startswith("_search_")}
            matching_entities.append(filtered_entity)
            
    # Return results
    result_text = f"Scanned {len(entities)} entities (offset {offset}, limit {limit}). Found {len(matching_entities)} matches:\n\n"
    result_text += str({"success": True, "result": matching_entities})
    
    if len(entities) == limit:
        result_text += f"\n\nTo continue searching, call this tool again with offset={offset + limit}."
    
    return [mcp.types.TextContent(type="text", text=result_text)]
