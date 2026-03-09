# CellarTracker MCP Server

An unofficial [CellarTracker](https://www.cellartracker.com/) [Remote MCP Server](https://modelcontextprotocol.io/) hosted on [Cloudflare Workers](https://workers.cloudflare.com/).

## Getting Started

[![Deploy to Workers](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/michaelahern/cellartracker-mcp/tree/main)

This will deploy your MCP server to a URL like: `cellartracker-mcp.<your-account>.workers.dev/mcp`

## Tools

- **get_cellar_stats** - Get aggregate statistics about your cellar.
- **search_wines** - Search wines in your cellar/collection with optional filters.
- **search_bottles** - Search individual bottles in your cellar with optional filters.
- **refresh_data** - Fetch the latest wine cellar inventory data from CellarTracker and store it in the database.
