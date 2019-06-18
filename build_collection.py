#! /usr/bin/env python
import click
import intake
import intake_esm


@click.command()
@click.argument('collection-input-definition')
@click.option('--overwrite-existing', default=False, is_flag=True)

def main(collection_input_definition, overwrite_existing):
    intake.open_esm_metadatastore(collection_input_definition,
                                  overwrite_existing=overwrite_existing)

if __name__ == '__main__':
    main()
