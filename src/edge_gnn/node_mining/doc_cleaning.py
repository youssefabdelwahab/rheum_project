import markdown 
from bs4 import BeautifulSoup
import re 





def normalize_latex_artifacts(raw_md_text: str) -> str:
    """
    Surgically cleans LaTeX artifacts without touching a single newline, 
    space, table, or reference chunk.
    """
    # This safely turns "\( \gamma \)" into " \gamma "
    text = re.sub(r'\\\(\s*(.*?)\s*\\\)', r'\1', raw_md_text)
    
    # 2. Map LaTeX macros to Unicode
    latex_to_unicode = {
        r'\alpha': 'α',
        r'\beta': 'β',
        r'\gamma': 'γ',
        r'\delta': 'δ',
        r'\epsilon': 'ε',
        r'\mu': 'μ',
        r'\pm': '±',
        r'\le': '≤',
        r'\ge': '≥',
        r'\times': '×'
    }
    
    for latex_macro, unicode_char in latex_to_unicode.items():
        text = text.replace(latex_macro, unicode_char)
        
    # We explicitly DO NOT strip whitespace or splitlines here.
    # The raw string is returned exactly as it was formatted.
    return text


def linearize_html_table(html_table_str: str, table_id: int) -> str:

    soup = BeautifulSoup(html_table_str, 'html.parser')
    rows = soup.find_all('tr')

    if not rows: 
        return ""

    header_row = rows[0]
    # Look for <th> tags, but fall back to <td> if the HTML is poorly formatted
    headers = [cell.get_text(strip=True) for cell in header_row.find_all(['th', 'td'])]
    
    # If the top-left cell is empty (very common in academic tables), give it a placeholder
    if headers and not headers[0]:
        headers[0] = "Metric"

    linearized_sentences = []

    for row in rows[1:]:
        cells = [cell.get_text(strip=True) for cell in row.find_all(['th', 'td'])]
        
        # Skip empty rows
        if not cells or all(c == "" for c in cells):
            continue
            
        # The first cell in the row is usually the variable being measured (e.g., "Age")
        row_label = cells[0]
        
        # 3. Construct the sentence for this specific row
        row_statements = []
        # Pair each remaining cell with its corresponding column header
        for i in range(1, len(cells)):
            # Protect against row length mismatch
            col_header = headers[i] if i < len(headers) else f"Column {i}"
            cell_value = cells[i]
            
            # Skip empty cells so we don't generate nonsense grammar
            if cell_value: 
                row_statements.append(f"the {col_header} was {cell_value}")
        
        # If we successfully extracted data, build the final sentence
        if row_statements:
            combined_statements = ", and ".join(row_statements)
            sentence = f"In Table {table_id}, regarding {row_label}, {combined_statements}."
            linearized_sentences.append(sentence)

    # 4. Join all the row sentences into a single paragraph
    return " ".join(linearized_sentences)   




def process_document(raw_md_text: str) -> str:
    """
    Cleans tables and references while strictly preserving the original 
    newlines and paragraph spacing of the raw markdown text.
    """
    # The regex looks for everything between <table> and </table>, spanning multiple lines
    table_pattern = re.compile(r'<table.*?>.*?</table>', re.IGNORECASE | re.DOTALL)
    
    # Extract all table matches first to avoid modifying the string while iterating
    extracted_tables = table_pattern.findall(raw_md_text)
    
    for i, raw_table_html in enumerate(set(extracted_tables)):
        # Pass ONLY the table to BeautifulSoup/your linearizer
        linearized_text = linearize_html_table(raw_table_html, table_id=i+1)
        
        # Replace the raw HTML table block in the text with the new sentences
        # Surrounded by double newlines to ensure it sits as its own paragraph
        raw_md_text = raw_md_text.replace(raw_table_html, f"\n\n{linearized_text}\n\n")

    # This looks for the word "REFERENCES" or "ACKNOWLEDGMENT" on its own line,
    # with or without Markdown '#' characters before it.
    ref_pattern = re.compile(r'\n#*\s*(REFERENCES|ACKNOWLEDGMENT|ACKNOWLEDGMENTS)[\s\r]*\n', re.IGNORECASE)
    ref_match = ref_pattern.search(raw_md_text)
    
    if ref_match:
        # Chop the string exactly where the reference/acknowledgment section begins
        raw_md_text = raw_md_text[:ref_match.start()]

    final_text = normalize_latex_artifacts(raw_md_text)
    
    # Smooth out any massive gaps (like 4+ newlines) created by removing tables
    final_text = re.sub(r'\n{3,}', '\n\n', final_text).strip()
    
    return final_text


def process_document_line_by_line(raw_md_text: str) -> str:
    """
    Processes the document line-by-line to guarantee 1:1 preservation 
    of the original line numbering and paragraph spacing.
    """
    # Split the document explicitly by newlines
    lines = raw_md_text.splitlines()
    
    processed_lines = []
    
    # State tracking variables
    in_table = False
    table_buffer = []
    table_count = 1
    
    # Iterate through every single line in the original document
    for line in lines:
        
        # If a line starts with References or Acknowledgments, we stop processing the rest of the file
        if re.match(r'^#*\s*(REFERENCES|ACKNOWLEDGMENT|ACKNOWLEDGMENTS)', line, re.IGNORECASE):
            break 
            
        # 2. TABLE CAPTURE (Entering a table)
        if '<table' in line.lower():
            in_table = True
            table_buffer.append(line)
            continue
            
        # If we are currently inside a table, keep capturing lines instead of writing them
        if in_table:
            table_buffer.append(line)
            
            # Check if this line is the end of the table
            if '</table>' in line.lower():
                in_table = False
                
                # Combine the buffered lines into one HTML string and linearize it
                table_html = "\n".join(table_buffer)
                linearized_text = linearize_html_table(table_html, table_id=table_count)
                table_count += 1
                
                # Append the newly generated sentences as a single line
                processed_lines.append(linearized_text)
                
                # Clear the buffer for the next table
                table_buffer = []
                
            continue # Skip to the next line without doing normal text processing

        # If it's just a regular line (or a blank line), clean its LaTeX and keep it exactly where it is
        clean_line = normalize_latex_artifacts(line)
        processed_lines.append(clean_line)
        
    # Rejoin the document using standard newlines to rebuild the exact vertical structure
    return "\n".join(processed_lines)




def clean_and_preserve_exact_structure(raw_md_text: str) -> str:
    # Split strictly by newline character to preserve exact vertical spacing
    lines = raw_md_text.split('\n') 
    output_lines = []
    
    in_table = False
    table_buffer = []
    table_counter = 1
    
    for line in lines:
        # If we hit a line starting with References, we stop appending to output_lines
        if re.match(r'^#*\s*(REFERENCES|ACKNOWLEDGMENT|ACKNOWLEDGMENTS)', line.strip(), re.IGNORECASE):
            break 
            
        # 2. TABLE ISOLATION
        if '<table' in line.lower():
            in_table = True
            table_buffer.append(line)
            continue
            
        if in_table:
            table_buffer.append(line)
            if '</table>' in line.lower():
                in_table = False
                
                # Convert the HTML block to sentences
                table_html = "\n".join(table_buffer)
                linearized = linearize_html_table(table_html, table_id=table_counter)
                table_counter += 1
                
                # Append the new sentences as a single line to replace the table block
                output_lines.append(linearized)
                table_buffer = []
            continue

        # Target only the exact \( ... \) pattern without touching surrounding spaces
        cleaned_line = re.sub(r'\\\(\s*(.*?)\s*\\\)', r'\1', line)
        
        replacements = {
            r'\alpha': 'α', r'\beta': 'β', r'\gamma': 'γ', 
            r'\delta': 'δ', r'\epsilon': 'ε', r'\mu': 'μ', 
            r'\pm': '±', r'\le': '≤', r'\ge': '≥', r'\times': '×'
        }
        for latex, unicode_char in replacements.items():
            cleaned_line = cleaned_line.replace(latex, unicode_char)
            
        # Append the line exactly as it is (preserves all original line lengths)
        output_lines.append(cleaned_line)
        
    # Rejoin the document using standard newlines to rebuild the file
    return '\n'.join(output_lines)