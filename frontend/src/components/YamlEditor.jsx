
import React from 'react';
import { motion } from 'framer-motion';
import { Textarea } from '@/components/ui/textarea';
import { Button } from '@/components/ui/button';
import { Upload, Download, Play } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';

const YamlEditor = ({ yamlString, onYamlChange, onParse, onImport, onExport }) => {
  const handleFileImport = (event) => {
    const file = event.target.files[0];
    if (file) {
      const reader = new FileReader();
      reader.onload = (e) => {
        onImport(e.target.result);
      };
      reader.readAsText(file);
    }
  };

  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.5 }}
    >
      <Card className="bg-slate-800/50 border-purple-500/50 shadow-xl backdrop-blur-md">
        <CardHeader className="flex flex-row items-center justify-between">
          <CardTitle className="text-2xl font-semibold bg-clip-text text-transparent bg-gradient-to-r from-purple-400 to-pink-500">
            YAML Configuration
          </CardTitle>
          <div className="flex space-x-2">
            <Button variant="outline" size="sm" onClick={onParse} className="bg-green-500/20 hover:bg-green-500/40 border-green-500 text-green-300">
              <Play className="mr-2 h-4 w-4" /> Apply
            </Button>
            <Button variant="outline" size="sm" onClick={() => document.getElementById('yaml-import').click()} className="bg-blue-500/20 hover:bg-blue-500/40 border-blue-500 text-blue-300">
              <Upload className="mr-2 h-4 w-4" /> Import
            </Button>
            <input type="file" id="yaml-import" accept=".yaml,.yml" style={{ display: 'none' }} onChange={handleFileImport} />
            <Button variant="outline" size="sm" onClick={onExport} className="bg-purple-500/20 hover:bg-purple-500/40 border-purple-500 text-purple-300">
              <Download className="mr-2 h-4 w-4" /> Export
            </Button>
          </div>
        </CardHeader>
        <CardContent>
          <Textarea
            value={yamlString}
            onChange={(e) => onYamlChange(e.target.value)}
            placeholder="Enter YAML configuration here..."
            className="min-h-[300px] lg:min-h-[calc(100vh-300px)] bg-slate-900/70 border-slate-700 text-slate-200 font-mono text-sm focus:ring-pink-500 focus:border-pink-500"
          />
        </CardContent>
      </Card>
    </motion.div>
  );
};

export default YamlEditor;
  