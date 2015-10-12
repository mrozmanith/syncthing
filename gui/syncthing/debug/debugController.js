angular.module('syncthing.debug')
.controller('DebugController', function ($scope, $http) {
	'use strict';

	$scope.enabled = {};
	$scope.facilities = {};
	$scope.log = [];

	$http.get(urlbase + '/system/debug').success(function (data) {
		$scope.enabled = data.enabled;
		$scope.facilities = data.facilities;
	});

	var lastLogLine;
	var repeatLoad = function () {
		$http.get(urlbase + '/system/log?since='+lastLogLine).success(function (data) {
			var msgs = data.messages;
			for (var i = 0; i < msgs.length; i++) {
				$scope.log.push(msgs[i].message);
			}

			lastLogLine = msgs[msgs.length - 1].when;
			setTimeout(repeatLoad, 2500);
		});
	};

	repeatLoad();
});
