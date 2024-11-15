# main.py

import uvicorn
import asyncio
from app import app
import global_vars


async def main():
    config = uvicorn.Config(app, host="0.0.0.0", port=8080, loop="asyncio")
    server = uvicorn.Server(config)

    # Run the server in the background
    server_task = asyncio.create_task(server.serve())

    # Wait for the shutdown event
    await global_vars.shutdown_event.wait()

    # Shutdown the server
    server.should_exit = True

    # Wait for the server to actually shutdown
    await server_task


if __name__ == "__main__":
    asyncio.run(main())
