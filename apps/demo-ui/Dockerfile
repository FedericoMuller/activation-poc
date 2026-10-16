# Contexto de build: raíz del monorepo
FROM node:22-alpine AS build
RUN npm i -g pnpm@9
WORKDIR /repo
COPY . .
RUN pnpm install --no-frozen-lockfile --filter "@poc/demo-ui..."
# La UI corre en el navegador del host, por eso apunta a localhost y no al nombre del contenedor.
ARG VITE_API_URL=http://localhost:3000
ENV VITE_API_URL=$VITE_API_URL
RUN pnpm --filter "@poc/demo-ui" build

FROM nginx:alpine
COPY --from=build /repo/apps/demo-ui/dist /usr/share/nginx/html
