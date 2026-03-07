import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { McpAgent } from 'agents/mcp';

export class MyMCP extends McpAgent {
    server = new McpServer({
        name: 'CellarTracker',
        version: '1.0.0'
    });

    async init() {
        this.server.registerTool('hello', {
            title: 'Hello World',
            description: 'Say Hello World!'
        },
        async () => ({
            content: [{ type: 'text', text: 'Hello World!' }]
        }));
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
