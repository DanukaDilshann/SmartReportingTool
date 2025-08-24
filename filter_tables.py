import re

def merge_unique_table_definitions(selected_modules, full_schema):
    seen_table_names = set()
    merged_schema_lines = []

    for module in full_schema:
        if module["name"] in selected_modules:
            lines = module["tables"].strip().splitlines()
            current_table = None
            current_table_block = []

            for line in lines:
                table_match = re.match(r"Table:\s*(\S+)", line)
                if table_match:
                    
                    if current_table and current_table.lower() not in seen_table_names:
                        merged_schema_lines.extend(current_table_block)
                        merged_schema_lines.append("") 
                        seen_table_names.add(current_table.lower())

                    current_table = table_match.group(1)
                    current_table_block = [line]
                else:
                    if current_table_block is not None:
                        current_table_block.append(line)

            if current_table and current_table.lower() not in seen_table_names:
                merged_schema_lines.extend(current_table_block)
                seen_table_names.add(current_table.lower())
                merged_schema_lines.append("")

    return "\n".join(merged_schema_lines)
