def main():

    url_to_scrape = "https://adstransparency.google.com/advertiser/AR16537964434459459585/creative/CR11320779767797514241"
    # url_to_scrape = "https://adstransparency.google.com/advertiser/AR14182191227838922753/creative/CR06756772009524330497"
    from playwright.sync_api import sync_playwright

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()

        # Navigate to the target URL
        url = url_to_scrape
        page.goto(url)

        # Wait for the iframe to load
        page.wait_for_selector("iframe")

        # Find the iframe and get its 'src' attribute
        iframe_element = page.query_selector("iframe")
        if iframe_element:
            iframe_src = iframe_element.get_attribute("src")
            print("Video iframe URL:", iframe_src)

        browser.close()


if __name__ == "__main__":
    main()
