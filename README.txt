4SIM CONTROL CENTER - WINDOWS

1. Extract ZIP.
2. Double-click start_dashboard.bat (Python must be installed).
3. Enter your 4SIM API key when prompted.
4. Open http://127.0.0.1:5050
5. Click Start Bot.

Do not expose port 5050 to the internet without authentication, HTTPS and appropriate access controls.
Optional: set DASHBOARD_TOKEN environment variable to require a token for all pages/API requests.
Cancellation success: accepts provider JSON {"status":200,"message":"Number cancelled successfully."}, as well as success=true and recognized success status strings. Successful transactions are not retried.
HTTP 400/401/403/404 are not retried; temporary errors may be retried up to 5 times. Failed transactions remain flagged until restart.
Logs redact API keys and do not print complete cancellation URLs. Rotate any API key previously exposed in logs.
Cancellation is accepted based on the provider's response, not independently verified against a second endpoint.
No live API test was performed.
