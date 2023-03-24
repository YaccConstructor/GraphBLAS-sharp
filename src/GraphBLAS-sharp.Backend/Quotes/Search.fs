namespace GraphBLAS.FSharp.Backend.Quotes

open Brahma.FSharp

module Search =
    module Bin =
        let byKey<'a> =
            <@ fun lenght sourceIndex (indices: ClArray<int>) (values: ClArray<'a>) ->

                let mutable leftEdge = 0
                let mutable rightEdge = lenght - 1

                let mutable result = None

                while leftEdge <= rightEdge do
                    let middleIdx = (leftEdge + rightEdge) / 2
                    let currentIndex = indices.[middleIdx]

                    if sourceIndex = currentIndex then
                        result <- Some values.[middleIdx]

                        rightEdge <- -1 // TODO() break
                    elif sourceIndex < currentIndex then
                        rightEdge <- middleIdx - 1
                    else
                        leftEdge <- middleIdx + 1

                result @>

        let byKey2<'a> =
            <@ fun lenght sourceIndex (rowIndices: ClArray<int>) (columnIndices: ClArray<int>) (values: ClArray<'a>) ->

                let mutable leftEdge = 0
                let mutable rightEdge = lenght - 1

                let mutable result = None

                while leftEdge <= rightEdge do
                    let middleIdx = (leftEdge + rightEdge) / 2

                    let currentIndex: uint64 =
                        ((uint64 rowIndices.[middleIdx]) <<< 32)
                        ||| (uint64 columnIndices.[middleIdx])

                    if sourceIndex = currentIndex then
                        result <- Some values.[middleIdx]

                        rightEdge <- -1 // TODO() break
                    elif sourceIndex < currentIndex then
                        rightEdge <- middleIdx - 1
                    else
                        leftEdge <- middleIdx + 1

                result @>

        /// <summary>
        /// Find lower position of item in array.
        /// </summary>
        let lowerPosition<'a when 'a: equality and 'a: comparison> =
            <@ fun lenght sourceItem (keys: 'a []) ->

                let mutable leftEdge = 0
                let mutable rightEdge = lenght - 1
                let mutable resultPosition = None

                while leftEdge <= rightEdge do
                    let currentPosition = (leftEdge + rightEdge) / 2
                    let currentKey = keys.[currentPosition]

                    if sourceItem = currentKey then
                        // remember positions and move left
                        resultPosition <- Some currentPosition

                        rightEdge <- currentPosition - 1
                    elif sourceItem < currentKey then
                        rightEdge <- currentPosition - 1
                    else
                        leftEdge <- currentPosition + 1

                resultPosition @>
