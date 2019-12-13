# Node.js LTS 10.x.x from Docker Hub
FROM node:10

# Create app directory
WORKDIR /usr/src/app

# Install app dependencies
# A wildcard is used to ensure both package.json AND package-lock.json are copied
# where available (npm@5+)
COPY package*.json ./
# RUN npm install

# Bundle app source
COPY . .

# Expose ports for app to bind to
# Note: ports can be exposed at runtime too with --expose or -p <port>:<port>
EXPOSE 4000

# Define run command
CMD [ "bin/archive-server" ]
