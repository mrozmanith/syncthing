angular.module('syncthing.debug')
    .directive('debugConsole', function () {
        return {
            restrict: 'A',
            templateUrl: 'syncthing/debug/debugConsoleView.html'
        };
});
