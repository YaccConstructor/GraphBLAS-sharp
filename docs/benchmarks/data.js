window.BENCHMARK_DATA = {
  "lastUpdate": 1678171382517,
  "repoUrl": "https://github.com/YaccConstructor/GraphBLAS-sharp",
  "entries": {
    "BFS": [
      {
        "commit": {
          "author": {
            "email": "71129570+kirillgarbar@users.noreply.github.com",
            "name": "Kirill",
            "username": "kirillgarbar"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "6a0b88c13ad6bfdb8b49f271641ef33a9194e449",
          "message": "Merge branch 'dev' into bfs",
          "timestamp": "2023-03-06T22:39:01+03:00",
          "tree_id": "6ac1e3c2895b2d212448bce7c538abe10e9b0eb9",
          "url": "https://github.com/kirillgarbar/GraphBLAS-sharp/commit/6a0b88c13ad6bfdb8b49f271641ef33a9194e449"
        },
        "date": 1678133371873,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "coAuthorsCiteseer.mtx",
            "value": 26976326.1,
            "unit": "ns",
            "range": "± 1188416.3809004356"
          },
          {
            "name": "hollywood-2009.mtx",
            "value": 244664916.75,
            "unit": "ns",
            "range": "± 427244.22641279944"
          },
          {
            "name": "roadNet-CA.mtx",
            "value": 276640576.8,
            "unit": "ns",
            "range": "± 856227.0199268674"
          },
          {
            "name": "wing.mtx",
            "value": 81482273.9,
            "unit": "ns",
            "range": "± 7462531.223758084"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "st087492@student.spbu.ru",
            "name": "kirillgarbar",
            "username": "kirillgarbar"
          },
          "committer": {
            "email": "st087492@student.spbu.ru",
            "name": "kirillgarbar",
            "username": "kirillgarbar"
          },
          "distinct": true,
          "id": "ee8129199ea4b34978b1717f07eeeca0b8f5a97c",
          "message": "Restore tools before build",
          "timestamp": "2023-03-07T02:25:51+03:00",
          "tree_id": "724cf3d61852fb6dc8b8e255a536264f4c813726",
          "url": "https://github.com/YaccConstructor/GraphBLAS-sharp/commit/ee8129199ea4b34978b1717f07eeeca0b8f5a97c"
        },
        "date": 1678171382164,
        "tool": "benchmarkdotnet",
        "benches": [
          {
            "name": "coAuthorsCiteseer.mtx",
            "value": 27438076.7,
            "unit": "ns",
            "range": "± 1297181.9540538252"
          },
          {
            "name": "hollywood-2009.mtx",
            "value": 243953769.44444445,
            "unit": "ns",
            "range": "± 369898.03864481056"
          },
          {
            "name": "roadNet-CA.mtx",
            "value": 386331568.3,
            "unit": "ns",
            "range": "± 547459.559564205"
          },
          {
            "name": "wing.mtx",
            "value": 74053240.7,
            "unit": "ns",
            "range": "± 6156789.947659613"
          }
        ]
      }
    ]
  }
}