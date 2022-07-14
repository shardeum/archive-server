# Node.js LTS 10.x.x from Docker Hub
FROM node:16.11.1

# Create app directory
WORKDIR /usr/src/app

# Install app dependencies
# A wildcard is used to ensure both package.json AND package-lock.json are copied
# where available (npm@5+)
COPY package*.json ./

# Bundle app source
COPY . .

# Install node_modules
RUN npm set unsafe-perm true
RUN npm install

# Expose ports for app to bind to
# Note: ports can be exposed at runtime too with --expose or -p <port>:<port>
# EXPOSE 4000

# Define run command
CMD [ "node", "build/server.js" ]
