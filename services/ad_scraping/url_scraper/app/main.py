from search_google_ads import search_google_ads
from extract_advertiser_id import extract_advertiser_id
import asyncio

user_input_url = "clasohlson.com"


async def main(user_input_url):
    # Example URL with no results
    try:
        # Await the result of the first function
        temp_url = await search_google_ads(user_input_url)

        # Proceed only if temp_url is valid
        if temp_url:
            final_url = await extract_advertiser_id(
                temp_url
            )  # Await the result of the second function
            return final_url
    except ValueError as e:
        print(e)  # Print only the error message


if __name__ == "__main__":
    ad_id = asyncio.run(
        main(user_input_url)
    )  # Run the main function and get the final URL
    print(f"The final URL is: {ad_id}")  # Print the final URL for confirmation
