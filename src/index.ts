import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { McpAgent } from 'agents/mcp';

export class MyMCP extends McpAgent {
    server = new McpServer({
        name: 'CellarTracker',
        version: '1.0.0'
    });

    async init() {
        this.ct_tool('wine', 'CellarTracker Wine List', 'Get your current wine library list from CellarTracker.', 'List');
        this.ct_tool('wine_bottles', 'CellarTracker Wine Bottles', 'Get your current wine bottle inventory from CellarTracker.', 'Inventory');
        this.ct_tool('wine_consumed', 'CellarTracker Wine Consumed', 'Get your consumed wine bottles from CellarTracker.', 'Consumed');
        this.ct_tool('wine_pending', 'CellarTracker Wine Pending', 'Get your wine inventory pending delivery from CellarTracker.', 'Pending');
        this.ct_tool('wine_purchases', 'CellarTracker Wine Purchases', 'Get your wine purchase history from CellarTracker.', 'Purchase');
    }

    async ct_tool(name: string, title: string, description: string, table: 'List' | 'Inventory' | 'Purchase' | 'Pending' | 'Consumed' | 'Availability') {
        this.server.registerTool(name, {
            title: title,
            description: description
        },
        async () => {
            const username = await this.env.CELLARTRACKER_USERNAME.get();
            const password = await this.env.CELLARTRACKER_PASSWORD.get();
            const url = `https://www.cellartracker.com/xlquery.asp?User=${encodeURIComponent(username)}&Password=${encodeURIComponent(password)}&Format=csv&Table=${encodeURIComponent(table)}`;
            const response = await fetch(url);

            if (!response.ok) {
                return {
                    content: [{ type: 'text', text: `Failed to fetch data from CellarTracker: ${response.status} ${response.statusText}` }],
                    isError: true
                };
            }

            return {
                content: [{ type: 'text', text: await response.text() }]
            };
        });
    }
}

export default {
    fetch(request: Request, env: Env, ctx: ExecutionContext) {
        const url = new URL(request.url);

        if (url.pathname === '/mcp') {
            return MyMCP.serve('/mcp').fetch(request, env, ctx);
        }

        return new Response('Not found', { status: 404 });
    }
};
