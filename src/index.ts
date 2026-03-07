import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { McpAgent } from 'agents/mcp';

export class MyMCP extends McpAgent {
    server = new McpServer({
        name: 'CellarTracker',
        version: '1.0.0'
    });

    async init() {
        this.server.registerTool('library', {
            title: 'CellarTracker Library',
            description: 'Fetches your entire wine library data from CellarTracker'
        },
        async () => {
            const username = await this.env.CELLARTRACKER_USERNAME.get();
            const password = await this.env.CELLARTRACKER_PASSWORD.get();
            const url = `https://www.cellartracker.com/xlquery.asp?User=${encodeURIComponent(username)}&Password=${encodeURIComponent(password)}&Format=csv`;
            const response = await fetch(url);
            if (!response.ok) {
                return {
                    content: [{ type: 'text', text: `Failed to fetch data from CellarTracker: ${response.status} ${response.statusText}` }],
                    isError: true
                };
            }
            const text = await response.text();
            return {
                content: [{ type: 'text', text }]
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
