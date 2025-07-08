
import React, { useState, useEffect, useCallback } from 'react';
import { applyNodeChanges, applyEdgeChanges } from 'reactflow';
import YamlEditor from '@/components/YamlEditor';
import NodeGraph from '@/components/NodeGraph';
import ParameterEditor from '@/components/ParameterEditor';
import { parseYamlToElements, elementsToYaml, initialYaml } from '@/lib/yamlParser';
import { useToast } from '@/components/ui/use-toast';
import { motion, AnimatePresence } from 'framer-motion';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from '@/components/ui/card';


const DataSciencePipelinePage = () => {
  const [yamlString, setYamlString] = useState(initialYaml);
  const [nodes, setNodes] = useState([]);
  const [edges, setEdges] = useState([]);
  const [selectedNode, setSelectedNode] = useState(null);
  const { toast } = useToast();

  const parseAndSetElements = useCallback((currentYaml) => {
    try {
      const { nodes: parsedNodes, edges: parsedEdges } = parseYamlToElements(currentYaml);
      setNodes(parsedNodes);
      setEdges(parsedEdges);
      setSelectedNode(null); 
      toast({
        title: "YAML Parsed Successfully!",
        description: "Graph updated with new configuration.",
        variant: "default",
      });
    } catch (error) {
      console.error("Parsing error:", error);
      toast({
        title: "YAML Parsing Error",
        description: error.message || "Could not parse YAML. Please check syntax.",
        variant: "destructive",
      });
    }
  }, [toast]);

  const sendYamlToBackend = async () => {
  try {
    const response = await fetch("http://localhost:8000/upload-yaml", {
      method: "POST",
      headers: {
        "Content-Type": "text/plain", // or "application/x-yaml"
      },
      body: yamlString,
    });

    const result = await response.json();
    toast({
      title: "YAML Sent",
      description: result.message,
    });
  } catch (error) {
    console.error("Error sending YAML:", error);
    toast({
      title: "Error",
      description: "Failed to send YAML to backend.",
      variant: "destructive",
    });
  }
};

  useEffect(() => {
    parseAndSetElements(yamlString);
  }, []); 

  const handleYamlChange = (newYaml) => {
    setYamlString(newYaml);
  };

  const handleParseYaml = () => {
    parseAndSetElements(yamlString);
  };
  
  const updateYamlFromGraph = useCallback(() => {
    if (nodes.length > 0) {
      const newYaml = elementsToYaml(nodes, edges);
      setYamlString(newYaml);
    }
  }, [nodes, edges, setYamlString]);

  const onNodesChange = useCallback(
    (changes) => {
      setNodes((nds) => applyNodeChanges(changes, nds));
      setTimeout(updateYamlFromGraph, 0); 
    },
    [setNodes, updateYamlFromGraph]
  );

  const onEdgesChange = useCallback(
    (changes) => {
      setEdges((eds) => applyEdgeChanges(changes, eds));
      setTimeout(updateYamlFromGraph, 0);
    },
    [setEdges, updateYamlFromGraph]
  );

  const handleNodeClick = useCallback((event, node) => {
    setSelectedNode(node);
  }, []);

  const handleUpdateNodeParams = useCallback((nodeId, updatedData) => {
    setNodes((prevNodes) =>
      prevNodes.map((n) =>
        n.id === nodeId ? { ...n, data: { ...n.data, ...updatedData } } : n
      )
    );
    setSelectedNode(prevNode => prevNode ? {...prevNode, data: {...prevNode.data, ...updatedData}} : null);
    setTimeout(updateYamlFromGraph, 0);
    toast({
      title: "Node Updated",
      description: `Parameters for node ${nodeId} saved.`,
    });
  }, [setNodes, updateYamlFromGraph, toast]);

  const handleCloseParameterEditor = () => {
    setSelectedNode(null);
  };

  const handleImportYaml = (importedYaml) => {
    setYamlString(importedYaml);
    parseAndSetElements(importedYaml);
    toast({
      title: "YAML Imported",
      description: "Configuration loaded from file.",
    });
  };

  const handleExportYaml = () => {
    const blob = new Blob([yamlString], { type: 'text/yaml;charset=utf-8;' });
    const link = document.createElement("a");
    const url = URL.createObjectURL(blob);
    link.setAttribute("href", url);
    link.setAttribute("download", "pipeline_config.yaml");
    link.style.visibility = 'hidden';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    toast({
      title: "YAML Exported",
      description: "Configuration saved to pipeline_config.yaml.",
    });
  };

  return (
    <div className="flex flex-col lg:flex-row gap-4 h-[calc(100vh-150px)] p-2">
      <motion.div 
        className="w-full lg:w-1/3"
        initial={{ opacity: 0, x: -50 }}
        animate={{ opacity: 1, x: 0 }}
        transition={{ duration: 0.5, ease: "easeOut" }}
      >
        <YamlEditor
          yamlString={yamlString}
          onYamlChange={handleYamlChange}
          onParse={handleParseYaml}
          onImport={handleImportYaml}
          onExport={handleExportYaml}
        />
      </motion.div>

      <motion.div 
        className="w-full lg:w-2/3 flex-grow h-[500px] lg:h-auto"
        initial={{ opacity: 0, scale: 0.9 }}
        animate={{ opacity: 1, scale: 1 }}
        transition={{ duration: 0.5, delay: 0.2, ease: "easeOut" }}
      >
        <NodeGraph
          initialNodes={nodes}
          initialEdges={edges}
          onNodesChangeToParent={onNodesChange}
          onEdgesChangeToParent={onEdgesChange}
          onNodeClick={handleNodeClick}
        />
        <button
    onClick={sendYamlToBackend}
    className="mt-2 bg-purple-600 text-white px-4 py-2 rounded hover:bg-purple-700"
  >
    Send YAML to Backend
  </button>
      </motion.div>
      
      <AnimatePresence>
        {selectedNode && (
          <ParameterEditor
            node={selectedNode}
            onUpdateNode={handleUpdateNodeParams}
            onClose={handleCloseParameterEditor}
          />
        )}
      </AnimatePresence>
       {!selectedNode && ( 
          <div className="hidden lg:block w-full md:w-1/3 lg:w-1/4 p-1">
             <Card className="bg-slate-800/50 border-purple-500/50 shadow-xl backdrop-blur-md h-full">
              <CardHeader>
                <CardTitle className="text-xl font-semibold text-purple-300">Node Parameters</CardTitle>
                <CardDescription className="text-slate-400">Select a node to view and edit its parameters.</CardDescription>
              </CardHeader>
              <CardContent>
                 <div className="flex items-center justify-center h-40">
                    <p className="text-slate-500">No node selected.</p>
                 </div>
              </CardContent>
            </Card>
          </div>
        )}
    </div>
  );
};

export default DataSciencePipelinePage;
  