from twisted.web import server, resource
from twisted.internet import reactor, endpoints
from twisted.enterprise import adbapi


class Counter(resource.Resource):
    isLeaf = True
    number_requests = 0

    def render_GET(self, request):
        self.number_requests += 1
        request.setHeader('content-type', 'text/plain')
        return "I am request #{}\n".format(self.number_requests)


endpoints.serverFromString(reactor, "tcp:5000").listen(server.Site(Counter()))
reactor.run()