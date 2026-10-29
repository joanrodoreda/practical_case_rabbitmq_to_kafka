"""Take the report screenshot of the RabbitMQ management UI (Figure 3).

Run from the project folder, with the brokers up:
    docker compose up -d
    python demo_queues.py
    pip install playwright
    playwright install chromium
    python screenshot_ui.py
Saves figures/management_ui.png
"""
from playwright.sync_api import sync_playwright

URL = "http://localhost:15672/"

with sync_playwright() as p:
    browser = p.chromium.launch()
    page = browser.new_page(viewport={"width": 1400, "height": 650}, device_scale_factor=2)
    page.goto(URL)
    page.fill("input[name=username]", "guest")
    page.fill("input[name=password]", "guest")
    page.click("input[type=submit]")
    page.wait_for_selector("#main", timeout=15000)
    page.goto(URL + "#/queues")
    page.wait_for_selector("text=demo-stream", timeout=15000)
    # New queues show "?" until their first stats arrive; the UI auto-refreshes every 5 s
    page.wait_for_function(
        "() => ![...document.querySelectorAll('table.list td')].some(td => td.innerText.trim() === '?')",
        timeout=60000)
    page.screenshot(path="figures/management_ui.png", full_page=False)
    browser.close()
print("Saved figures/management_ui.png")