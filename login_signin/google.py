import requests

url = "https://ad-libraries.p.rapidapi.com/google/search"

querystring = {
    "domain": "nike.com",
    "country_code": "SE",
    "format": "ALL",
    "limit": "40",
}

headers = {
    "x-rapidapi-key": "968da7f50dmsh6ba410944fce480p1867dcjsna94f6bbc9d76",
    "x-rapidapi-host": "ad-libraries.p.rapidapi.com",
}

response = requests.get(url, headers=headers, params=querystring)

print(response.json())
