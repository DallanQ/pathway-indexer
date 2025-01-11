import os
import re

import pandas as pd


def calendar_format(input_directory, metadata_csv):
    all_links_path = os.path.join(input_directory, metadata_csv)
    all_links_df = pd.read_csv(all_links_path)

    target_url = "https://student-services.catalog.prod.coursedog.com/studentservices/academic-calendar"
    row = all_links_df[all_links_df["URL"] == target_url]

    if not row.empty:
        filename = row.iloc[0]["filename"]
        file_path = os.path.join(input_directory, "out/from_html", filename + ".md")

        if os.path.exists(file_path):
            print(f"Processing academic calendar file: {file_path}")

            try:
                # Read the file
                with open(file_path, encoding="utf-8") as file:
                    content = file.read()

                # Transforming the content
                updated_content = transform_document(content)

                # Combining transformed content and parsed tables
                combined_content = updated_content + "\n\n"

                # Save changes
                with open(file_path, "w", encoding="utf-8") as file:
                    file.write(combined_content)

                print(f"Calendar tables transformed successfully in {file_path}")
            except Exception as e:
                print(f"Error transforming calendar tables: {e!s}")
                raise  # Re-raise the exception for debugging
        else:
            print(f"Academic calendar file not found: {file_path}")
    else:
        print(f"Academic calendar URL not found in {metadata_csv}")


def transform_document(content):
    # Regular expressions for searching headers and tables
    pattern = r"(### (Spring|Winter|Fall) \d{4}\n.*?(\n\n|\Z))"
    # Find all header and table matches.
    matches = re.finditer(pattern, content, re.DOTALL)

    # Regular expression for search the year
    year_match = re.search(r"# (\d{4}) Start/End Dates \\& Deadlines", content)
    year = year_match.group(1)

    updated_content = content  # Copy the original content
    parsed_results = []

    # Process the extracted tables
    for match in matches:
        section = match.group(0)
        header, table = section.split("\n", 1) if "\n" in section else (section, "")
        # Pass the table and the year
        parsed_table = parse_markdown_table(table.strip(), year)
        # parsed_results.append(parsed_table)

        # Replace the original section with the transformed content
        replacement = f"{parsed_table}\n\n"
        updated_content = updated_content.replace(section, replacement, 1)

    # Return updated content with replaced tables
    return updated_content.strip()


def parse_markdown_table(markdown_text, year):
    """Convert markdown table to bullet point format, handling any term numbers."""
    try:
        # Split input into lines and get the title
        lines = markdown_text.strip().split("\n")
        title = lines[0].strip()
        first_line = title.split("|")

        semester = ""
        for i in first_line:
            print(i)
            if "Term 1" in i:
                semester = "Winter"
                break  # Sale del bucle al encontrar coincidencia
            elif "Term 3" in i:
                semester = "Spring"
                break
            elif "Term 5" in i:
                semester = "Fall"
                break

        # Find the table lines
        table_lines = [line.strip() for line in lines if "|" in line]

        # Remove the separator line (the one with dashes)
        table_lines = [line for line in table_lines if line.replace("|", "").replace("-", "").strip() != ""]

        if len(table_lines) < 2:  # We need at least headers and a data row
            return markdown_text

        # Parse headers and data
        headers = [col.strip().replace("*", "") for col in table_lines[0].split("|")[1:-1]]

        # Get term numbers from headers
        term_numbers = []
        for header in headers[1:3]:  # Look at Term X columns
            try:
                # Look for any number in the header
                numbers = re.findall(r"\d+", header)
                if numbers:
                    term_numbers.append(numbers[0])
            except:
                pass

        # If no term numbers are found, return the original text
        if len(term_numbers) < 2:
            return markdown_text

        data = []
        for line in table_lines[1:]:
            cols = [col.strip().replace("*", "") for col in line.split("|")[1:-1]]
            if len(cols) >= 4:  # Make sure we have all the necessary columns
                data.append(cols)

        if not data:  # If there is no data, return the original text
            return markdown_text

        # Convert to bullet point format
        result = []

        # Process first term
        result.append(f"### Term {term_numbers[0]}:")
        for row in data:
            deadline = row[0].strip()
            value = row[1].strip()
            if deadline and value:  # Solo agregar si tenemos tanto deadline como valor
                result.append(f"- {deadline}: {value}")

        # Add blank line
        result.append("")

        # Process second term
        result.append(f"### Term {term_numbers[1]}:")
        for row in data:
            deadline = row[0].strip()
            value = row[2].strip()
            if deadline and value:
                result.append(f"- {deadline}: {value}")

        # Add blank line
        result.append("")

        # Process Semester
        if "Semester" in markdown_text:
            result.append(f"### Semester ({semester} {year}):")
            for row in data:
                deadline = row[0].strip()
                value = row[3].strip()
                # Add semester name to Start and End
                if deadline == "Start":
                    deadline = f"Start {semester}"
                elif deadline == "End":
                    deadline = f"End {semester}"
                result.append(f"- {deadline}: {value}")

        return "\n".join(result)
    except Exception as e:
        print(f"Error processing table: {e!s}")
        return markdown_text  # In case of error, return the original text
