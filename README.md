# Portfólio Minimalista - Engenharia de Dados

Bem-vindo ao repositório do meu portfólio pessoal! Este projeto foi desenvolvido para ser extremamente rápido, minimalista e construído sem o uso de Javascript complexo no frontend, utilizando **Astro**, **CSS Nativo** e suporte a **Markdown** para a escrita de projetos.

## 🚀 Como Rodar Localmente

Para iniciar o projeto em seu ambiente local, siga os passos abaixo:

1. Instale as dependências:
```bash
npm install
```

2. Inicie o servidor de desenvolvimento:
```bash
npm run dev
```

Seu site estará disponível em `http://localhost:4321`.

## 📁 Estrutura do Projeto

O Astro usa uma estrutura de roteamento e componentes baseada em diretórios. Aqui está um guia prático de onde encontrar e alterar as coisas:

```text
├── public/                 # Arquivos estáticos (ex: favicon.svg, curriculo.pdf)
├── src/
│   ├── components/         # Componentes Astro (Header, Hero, Botões, etc.)
│   ├── content/
│   │   └── projetos/       # SEUS PROJETOS FICAM AQUI! (Arquivos .md)
│   ├── layouts/            # O layout global e configurações de CSS raiz (Modo Dark/Light)
│   └── pages/              # Rotas do seu site
│       ├── index.astro     # Página Inicial (Hero Section)
│       ├── about.astro     # Página "Quem sou eu"
│       └── projetos/       # Página de listagem e detalhe dinâmico dos projetos
└── README.md
```

## 📝 Como adicionar um Novo Projeto

Para adicionar um novo projeto ao portfólio, é extremamente simples:

1. Vá até a pasta `src/content/projetos/`.
2. Crie um novo arquivo `.md` (exemplo: `meu-pipeline-de-dados.md`).
3. Adicione o "Frontmatter" (cabeçalho de configuração) no topo do arquivo. **Importante:** Agora as imagens e a descrição são opcionais!

```yaml
---
title: Nome do seu Projeto
publishDate: 2026-05-01 00:00:00
tags:
  - Python
  - SQL
  - AWS

# Campos Opcionais (Descomente se for usar):
# github: https://github.com/IasmimHorrana/meu-projeto
# description: Breve descrição do projeto em uma linha.
# img: /assets/uma-imagem.jpg
# img_alt: Imagem descritiva
---
```

4. Escreva o conteúdo do seu projeto usando Markdown logo abaixo do cabeçalho. Ele suporta subtítulos, negrito, links e blocos de código!

## 🎨 Temas e Estilo

Este projeto tem foco no **Dark Theme** por padrão para dar uma aparência moderna, porém possui suporte completo a **Light Theme**. 
O botão de troca de tema está localizado no topo esquerdo, no componente `<Header />` que adiciona ou remove a classe `theme-light` no `<html>`.

Feito com 💖 e Astro!
