import {
  FragmentDefinitionNode,
  GraphQLResolveInfo,
  SelectionSetNode,
} from "graphql";

const addFieldNamesFromSelectionSet = (
  fieldNames: Set<string>,
  selectionSet: SelectionSetNode,
  fragments: { [key: string]: FragmentDefinitionNode }
): void => {
  selectionSet.selections.forEach((selection) => {
    if (selection.kind === "FragmentSpread") {
      addFieldNamesFromSelectionSet(
        fieldNames,
        fragments[selection.name.value].selectionSet,
        fragments
      );
    } else if (selection.kind === "InlineFragment") {
      addFieldNamesFromSelectionSet(
        fieldNames,
        selection.selectionSet,
        fragments
      );
    } else {
      fieldNames.add(selection.name.value);
    }
  });
};

export const getRequestedFields = (
  info: Pick<GraphQLResolveInfo, "fieldNodes" | "fragments">
): Set<string> => {
  const fieldNames = new Set<string>();

  info.fieldNodes.forEach((fieldNode) => {
    if (fieldNode.selectionSet) {
      addFieldNamesFromSelectionSet(
        fieldNames,
        fieldNode.selectionSet,
        info.fragments
      );
    }
  });

  return fieldNames;
};
