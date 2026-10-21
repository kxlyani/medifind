FROM nginx:1.27-alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
# The website: three pages, a shared stylesheet, and the bundled font
COPY site/ /usr/share/nginx/html/
EXPOSE 80
