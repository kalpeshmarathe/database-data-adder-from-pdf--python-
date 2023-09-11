import pdfplumber
import pandas as pd
import psycopg2
import gdown



# Function to extract data from the PDF
def extract_data_from_pdf(pdf_file):
    data = []
    with pdfplumber.open(pdf_file) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            lines = text.split('\n')
            for line in lines:
                parts = line.strip().split()
                if len(parts) == 2:
                    company_name, career_link = parts
                    data.append({'company_name': company_name, 'career_link': career_link})
    return data

# Function to update the "company" table in PostgreSQL
def update_company_table(data):
    conn = psycopg2.connect(
        dbname="dbname",
        user="dbusername",
        password="",
        host=""
    )
    cursor = conn.cursor()

    for item in data:
        company_name = item['company_name']
        career_link = item['career_link']
        company_name = company_name[:255]
        career_link = career_link[:255]
        # Check if the company exists in the table
        cursor.execute("SELECT * FROM company WHERE name = %s;", (company_name,))
        existing_company = cursor.fetchone()

        if existing_company:
            # If the company exists, update the career_page column
            cursor.execute("UPDATE company SET careerpage = %s WHERE name = %s;", (career_link, company_name))
        else:
            # If the company doesn't exist, insert a new record
            cursor.execute("INSERT INTO company (name, careerpage) VALUES (%s, %s);", (company_name, career_link))
    print("successfull")
    conn.commit()
    conn.close()

if __name__ == "__main__":
    pdf_file = "pdfname"  # Local path to save the downloaded PDF
    data = extract_data_from_pdf(pdf_file)
    update_company_table(data)
