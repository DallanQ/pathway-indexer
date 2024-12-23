def parse_markdown_table(markdown_text):
    """Convert markdown table to bullet point format, handling any term numbers."""
    # Split input into lines and get the title
    lines = markdown_text.strip().split("\n")
    title = lines[0].strip()
    semester = title.split()[1]  # Get "Winter", "Spring", etc.

    # Find the table lines
    table_lines = [line.strip() for line in lines if "|" in line]

    # Remove the separator line (the one with dashes)
    table_lines = [line for line in table_lines if line.replace("|", "").replace("-", "").strip() != ""]

    # Parse headers and data
    headers = [col.strip().replace("*", "") for col in table_lines[0].split("|")[1:-1]]

    # Get term numbers from headers
    term_numbers = []
    for header in headers[1:3]:  # Look at Term X columns
        try:
            term_number = header.split()[-1]  # Get the number part
            term_numbers.append(term_number)
        except:
            pass

    data = []
    for line in table_lines[1:]:
        row = [col.strip().replace("*", "") for col in line.split("|")[1:-1]]
        data.append(row)

    # Convert to bullet point format
    result = []

    # Process first term
    result.append(f"### Term {term_numbers[0]}:")
    for row in data:
        deadline = row[0].strip()
        value = row[1].strip()
        result.append(f"- {deadline}: {value}")

    # Add blank line
    result.append("")

    # Process second term
    result.append(f"### Term {term_numbers[1]}:")
    for row in data:
        deadline = row[0].strip()
        value = row[2].strip()
        result.append(f"- {deadline}: {value}")

    # Add blank line
    result.append("")

    # Process Semester
    result.append(f"### Semester ({semester}):")
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


# Example usage
def convert_calendar_format(input_text):
    return parse_markdown_table(input_text)


input_text = """### Winter 2025

| **Date/Deadline**            | **Term 1**                     | **Term 2**                     | **Semester**                   |
|------------------------------|---------------------------------|---------------------------------|---------------------------------|
| **Start**                    | **Monday, January 6, 2025**    | **Monday, March 3, 2025**      | **Monday, January 6, 2025**    |
| Financial Holds Applied       | Sunday, November 24, 2024      | Sunday, January 26, 2025       | Sunday, November 24, 2024      |
| Add Course Deadline           | Monday, January 6, 2025        | Monday, March 3, 2025          | Monday, January 6, 2025        |
| Tuition Discount Deadline      | Monday, January 6, 2025        | Monday, March 3, 2025          | Monday, January 6, 2025        |
| Drop/Auto-Drop Deadline       | Monday, January 13, 2025       | Monday, March 10, 2025         | Monday, January 13, 2025       |
| Last Day for a Refund         | Monday, January 13, 2025       | Monday, March 10, 2025         | Monday, January 13, 2025       |
| Payment Deadline               | Sunday, January 26, 2025       | Sunday, March 23, 2025         | Sunday, January 26, 2025       |
| Late Fees Applied             | Monday, January 27, 2025       | Monday, March 24, 2025         | Monday, January 27, 2025       |
| Last Day to Withdraw          | Monday, February 3, 2025       | Monday, March 31, 2025         | Monday, March 31, 2025         |
| Grades Available              | Thursday, February 27, 2025    | Thursday, April 24, 2025       | Thursday, April 24, 2025       |
| **End**                      | **Saturday, February 22, 2025** | **Saturday, April 19, 2025**   | **Saturday, April 19, 2025**   |
...
"""

formatted_text = convert_calendar_format(input_text)
print(formatted_text)

print("\n\n")

input_text = """### Spring 2025

| **Date/Deadline**            | **Term 3**                     | **Term 4**                     | **Semester**                   |
|------------------------------|---------------------------------|---------------------------------|---------------------------------|
| **Start**                    | **Monday, May 5, 2025**        | **Monday, June 30, 2025**      | **Monday, May 5, 2025**        |
| Financial Holds Applied       | Sunday, March 23, 2025         | Sunday, May 25, 2025           | Monday, May 5, 2025            |
| Add Course Deadline           | Monday, May 5, 2025            | Monday, June 30, 2025          | Monday, March 24, 2025         |
| Tuition Discount Deadline      | Monday, May 5, 2025            | Monday, June 30, 2025          | Monday, May 5, 2025            |
| Drop/Auto-Drop Deadline       | Monday, May 12, 2025           | Monday, July 7, 2025           | Monday, May 5, 2025            |
| Last Day for a Refund         | Monday, May 12, 2025           | Monday, July 7, 2025           | Monday, May 12, 2025           |
| Payment Deadline               | Sunday, May 25, 2025           | Sunday, July 20, 2025          | Monday, May 12, 2025           |
| Late Fees Applied             | Monday, May 26, 2025           | Monday, July 21, 2025          | Sunday, May 25, 2025           |
| Last Day to Withdraw          | Monday, June 2, 2025           | Monday, July 28, 2025          | Monday, July 28, 2025          |
| Grades Available              | Thursday, June 26, 2025        | Thursday, August 21, 2025      | Thursday, August 21, 2025      |
| **End**                      | **Saturday, June 21, 2025**    | **Saturday, August 16, 2025**  | **Saturday, August 16, 2025**  |
"""

formatted_text = convert_calendar_format(input_text)
print(formatted_text)


print("\n\n")


input_text = """### Fall 2025

| **Date/Deadline**            | **Term 5**                     | **Term 6**                     | **Semester**                   |
|------------------------------|---------------------------------|---------------------------------|---------------------------------|
| **Start** | **Monday, September 1, 2025** | **Monday, October 27, 2025** |
| --- | --- | --- |
| **Monday, September 1, 2025** |  |  |
| Financial Holds Applied | Sunday, July 20, 2025 | Sunday, September 21, 2025 |
| Sunday, July 20, 2025 |  |  |
| Add Course Deadline | Monday, September 1, 2025 | Monday, October 27, 2025 |
| Monday, September 1, 2025 |  |  |
| Tuition Discount Deadline | Monday, September 1, 2025 | Monday, October 27, 2025 |
| Monday, September 1, 2025 |  |  |
| Drop/Auto-Drop Deadline | Monday, September 8, 2025 | Monday, November 3, 2025 |
| Monday, September 8, 2025 |  |  |
| Last Day for a Refund | Monday, September 8, 2025 | Monday, November 3, 2025 |
| Monday, September 8, 2025 |  |  |
| Payment Deadline | Sunday, September 21, 2025 | Sunday, November 16, 2025 |
| Sunday, September 21, 2025 |  |  |
| Late Fees Applied | Monday, September 22, 2025 | Monday, November 17, 2025 |
| Monday, September 22, 2025 |  |  |
| Last Day to Withdraw | Monday, September 29, 2025 | Monday, November 24, 2025 |
| Monday, November 24, 2025 |  |  |
| Grades Available | Thursday, October 23, 2025 | Thursday, December 18, 2025 |
| Thursday, December 18, 2025 |  |  |
| **End** | **Saturday, October 18, 2025** | **Saturday, December 13, 2025** |
| **Saturday, December 13, 2025** |  |  |
"""

formatted_text = convert_calendar_format(input_text)
print(formatted_text)
