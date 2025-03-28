# Use Node.js for frontend build
FROM node:20-alpine AS build

# Set working directory
WORKDIR /app

# Copy package.json and package-lock.json
COPY package.json package-lock.json ./

# Install dependencies
RUN npm install --frozen-lockfile

# Copy frontend source code
COPY . .

# Build the React app
RUN npm run build

# Serve with Nginx
FROM nginx:alpine
COPY --from=build /app/dist /usr/share/nginx/html

# Expose port for frontend
EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
