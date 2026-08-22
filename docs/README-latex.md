# Compilar la documentación

Este proyecto usa LaTeX con TikZ para los diagramas (versionables en git,
a diferencia de imágenes pegadas).

## Instalación

**Arch / Manjaro**
```bash
sudo pacman -S texlive-latex texlive-latexrecommended \
               texlive-latexextra texlive-pictures texlive-fontsrecommended \
               texlive-langspanish
```

**Debian / Ubuntu**
```bash
sudo apt install texlive-latex-recommended texlive-latex-extra \
                 texlive-pictures texlive-fonts-recommended texlive-lang-spanish
```

**Fedora**
```bash
sudo dnf install texlive-scheme-medium texlive-babel-spanish
```

## Compilar

```bash
make pdf      # docs/arquitectura.pdf  — documento de arquitectura
make slides   # docs/presentacion.pdf  — diapositivas Beamer
```

Ambos requieren dos pasadas de `pdflatex` (el Makefile ya lo hace) para
resolver referencias cruzadas.

## Sin instalar nada

Si prefieren no instalar TeX Live, suban los `.tex` a
[Overleaf](https://overleaf.com) — compila sin configuración adicional y
permite trabajo simultáneo entre los integrantes del equipo.

## Paquetes usados

`babel` (español) · `tikz` + librerías `shapes.geometric`, `arrows.meta`,
`positioning`, `calc` · `booktabs` · `xcolor` · `hyperref` · `fancyhdr` ·
`enumitem` · `beamer` (tema `seabird`)
