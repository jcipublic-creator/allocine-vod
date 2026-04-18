#!/usr/bin/env node
// Synchronise le fichier userdata.json local vers le serveur Railway
const fs   = require('fs');
const path = require('path');

const SERVER = process.argv[2] || 'https://allocine-vod-production.up.railway.app';
const FILE   = path.join(__dirname, 'userdata.json');

async function main() {
  if (!fs.existsSync(FILE)) { console.error('userdata.json introuvable'); process.exit(1); }
  const data = JSON.parse(fs.readFileSync(FILE, 'utf8'));
  const ids  = Object.keys(data);
  console.log(`📤 Synchronisation de ${ids.length} entrées vers ${SERVER}…`);

  let ok = 0, err = 0;
  for (const id of ids) {
    try {
      const res = await fetch(`${SERVER}/api/userdata`, {
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify({ id, ...data[id] })
      });
      if (res.ok) { ok++; process.stdout.write('.'); }
      else        { err++; process.stdout.write('x'); }
    } catch(e) {
      err++;
      process.stdout.write('!');
    }
  }
  console.log(`\n✅ ${ok} OK  ❌ ${err} erreurs`);
}

main();
