import requests
from bs4 import BeautifulSoup
import json


def webscraper():
    print("starting")
    url = "https://www.nike.com/se"  # Replace this with the actual URL

    # Send a GET request to fetch the raw HTML content
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the HTML content
        soup = BeautifulSoup(response.content, "html.parser")
        print("scraped")

        # Create a dictionary to store the extracted data
        extracted_data = {}

        # Example 1: Get the page title
        extracted_data["title"] = (
            soup.title.text.strip() if soup.title else "No title found"
        )

        # Example 2: Get all <h1>, <h2>, <h3> headers (sales or campaigns may be in headers)
        headers = soup.find_all(["h1", "h2", "h3"])
        extracted_data["headers"] = [header.text.strip() for header in headers]

        # Example 3: Get all <div> elements (sales banners are often in <div> elements)
        divs = soup.find_all("div")
        extracted_data["divs"] = [div.text.strip() for div in divs if div.text.strip()]

        # Example 4: Get all text content (in case sales or campaigns are mentioned in plain text)
        extracted_data["all_text"] = soup.get_text(separator=" ").strip()

        # Example 5: Store links (sometimes promotional links are used)
        links = soup.find_all("a")
        extracted_data["links"] = [
            {"text": link.text.strip(), "url": link.get("href")}
            for link in links
            if link.get("href")
        ]

        # Save the extracted data to a JSON file so you can share it
        with open("scraped_data.json", "w", encoding="utf-8") as f:
            json.dump(extracted_data, f, ensure_ascii=False, indent=4)

        print("Scraping complete. Data saved to 'scraped_data.json'.")
        return extracted_data

    else:
        print(f"Failed to retrieve page. Status code: {response.status_code}")


data = webscraper()
print(data)
