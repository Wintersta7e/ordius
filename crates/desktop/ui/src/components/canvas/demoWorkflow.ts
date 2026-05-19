// Hand-rolled workflow + node-type catalog used as a stand-in when
// the editor runs outside a Tauri host (plain Vite preview in a
// browser, no engine to query). Lets the visual loop work without
// a real backend; never used inside Tauri.

import type { NodeType, PortDef, Workflow } from "../../engine/types";

function port(name: string, type: PortDef["type"] = "string"): PortDef {
  return { name, type, required: false };
}

export const DEMO_NODE_TYPES: NodeType[] = [
  {
    id: "delay",
    name: "Delay",
    category: "control",
    tags: [],
    icon: "clock",
    description: "Sleep N milliseconds",
    inputs: [],
    outputs: [],
    config: [],
    execution: {
      backend: "in_process",
      command: [],
      env: {},
      output_parse: "text",
      output_map: {},
    },
  },
  {
    id: "transform",
    name: "Transform",
    category: "data",
    tags: [],
    icon: "shuffle",
    description: "JSONPath / regex extraction and replacement",
    inputs: [port("input")],
    outputs: [port("text")],
    config: [],
    execution: {
      backend: "in_process",
      command: [],
      env: {},
      output_parse: "text",
      output_map: {},
    },
  },
  {
    id: "http",
    name: "HTTP",
    category: "integration",
    tags: [],
    icon: "globe",
    description: "Make an HTTP request",
    inputs: [port("body")],
    outputs: [
      { name: "status", type: "number", required: false },
      { name: "body", type: "json", required: false },
    ],
    config: [],
    execution: {
      backend: "in_process",
      command: [],
      env: {},
      output_parse: "text",
      output_map: {},
    },
  },
  {
    id: "shell",
    name: "Shell",
    category: "execution",
    tags: [],
    icon: "terminal",
    description: "Run a shell command",
    inputs: [port("in")],
    outputs: [
      port("text"),
      { name: "exit_code", type: "number", required: false },
    ],
    config: [],
    execution: {
      backend: "subprocess",
      command: [],
      env: {},
      output_parse: "text",
      output_map: {},
    },
  },
  {
    id: "llm",
    name: "LLM",
    category: "llm",
    tags: [],
    icon: "sparkles",
    description: "OpenAI-compatible chat completion",
    inputs: [port("prompt")],
    outputs: [port("text")],
    config: [],
    execution: {
      backend: "in_process",
      command: [],
      env: {},
      output_parse: "text",
      output_map: {},
    },
  },
];

export const DEMO_WORKFLOW: Workflow = {
  id: "demo",
  name: "demo-workflow",
  schema_version: 1,
  variables: {},
  triggers: [{ type: "manual" }],
  nodes: [
    {
      id: "wait",
      type: "delay",
      name: "wait 100ms",
      config: { ms: 100 },
      pos: { x: 60, y: 80 },
      continue_on_error: false,
    },
    {
      id: "fetch",
      type: "http",
      name: "fetch readme",
      config: {},
      pos: { x: 340, y: 80 },
      continue_on_error: false,
    },
    {
      id: "summarise",
      type: "llm",
      name: "summarise body",
      config: {},
      pos: { x: 660, y: 80 },
      continue_on_error: false,
    },
    {
      id: "format",
      type: "transform",
      name: "format markdown",
      config: { op: "template" },
      pos: { x: 660, y: 320 },
      continue_on_error: false,
    },
    {
      id: "publish",
      type: "shell",
      name: "git commit",
      config: { command: "git commit -am 'auto'" },
      pos: { x: 980, y: 320 },
      continue_on_error: false,
    },
  ],
  edges: [
    {
      id: "e1",
      from_node_id: "wait",
      from_port: "out",
      to_node_id: "fetch",
      to_port: "body",
      edge_type: "forward",
    },
    {
      id: "e2",
      from_node_id: "fetch",
      from_port: "body",
      to_node_id: "summarise",
      to_port: "prompt",
      edge_type: "forward",
    },
    {
      id: "e3",
      from_node_id: "summarise",
      from_port: "text",
      to_node_id: "format",
      to_port: "input",
      edge_type: "forward",
    },
    {
      id: "e4",
      from_node_id: "format",
      from_port: "text",
      to_node_id: "publish",
      to_port: "in",
      edge_type: "forward",
    },
  ],
};
