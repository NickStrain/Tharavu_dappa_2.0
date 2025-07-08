
import React from 'react';
import DataSciencePipelinePage from '@/pages/DataSciencePipelinePage';
import { Toaster } from '@/components/ui/toaster';
import { ThemeProvider } from '@/components/ThemeProvider';

function App() {
  return (
    <ThemeProvider defaultTheme="dark" storageKey="vite-ui-theme">
      <div className="min-h-screen bg-gradient-to-br from-slate-900 via-purple-900 to-slate-900 text-foreground p-4 flex flex-col">
        <header className="mb-6">
          <h1 className="text-4xl font-bold text-center bg-clip-text text-transparent bg-gradient-to-r from-purple-400 via-pink-500 to-red-500 py-2">
            Tharavu Dappa
          </h1>
        </header>
        <main className="flex-grow">
          <DataSciencePipelinePage />
        </main>
        <footer className="mt-8 text-center text-sm text-muted-foreground">
          <p></p>
        </footer>
      </div>
      <Toaster />
    </ThemeProvider>
  );
}

export default App;
  