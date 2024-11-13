from search_google_ads import search_google_ads
from extract_advertiser_id import extract_advertiser_id
import asyncio
import logging


async def main(user_input_url):
    # Example URL with no results
    try:
        # Await the result of the first function
        temp_url = await search_google_ads(user_input_url)

        # Proceed only if temp_url is valid
        if temp_url:
            ad_id = await extract_advertiser_id(
                temp_url
            )  # Await the result of the second function
            return ad_id
    except ValueError as e:
        logging.error(e)


if __name__ == "__main__":
    ad_id = asyncio.run(main("user_input_url"))
