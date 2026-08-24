# Documentos web

Versiones HTML de los documentos del proyecto. Se abren con doble clic
o sirviendo la carpeta:

```bash
python3 -m http.server 4180 --directory docs/web
```

| Archivo | Para qué | Publicado |
|---|---|---|
| `mapa-interactivo.html` | Diagrama clicable: qué es cada pieza y cómo se conecta | [ver](https://claude.ai/code/artifact/6c07e92c-4478-4c48-b4b4-5b5a8db672c5) |
| `entrega.html` | Documento de entrega: los 4 criterios de la rúbrica | [ver](https://claude.ai/code/artifact/3c1aac00-ec4a-4328-9499-7013678e96d8) |
| `guia-conceptual.html` | Los conceptos explicados para el equipo | [ver](https://claude.ai/code/artifact/2cb3191a-84a5-45d9-9e0c-98e5ff143031) |
| `stack.html` | Ficha técnica: versiones, puertos, comandos | [ver](https://claude.ai/code/artifact/9825080a-4c59-441d-b62e-baabfc288617) |
| `divulgacion.html` | Explicación sin jerga, con diagramas | [ver](https://claude.ai/code/artifact/5a092670-dc72-4ce0-b22a-47d97e078890) |

`entrega.html` está pensado para imprimir: `Ctrl+P` → *Guardar como PDF*
respeta los colores y evita cortes entre secciones.

La versión LaTeX del documento de entrega está en `../arquitectura.tex`
y se compila con `make pdf`.
