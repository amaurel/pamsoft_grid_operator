"""
Tercen Pamsoft Grid Operator - Python Implementation
Refactored version with MATLAB handling parallelization
"""

import os
import sys
import json
import subprocess
import time
import tempfile
from pathlib import Path
import pandas as pd

# Tercen imports
from tercen.client import context as ctx


class PamsoftGridOperator:
    """Python implementation of pamsoft_grid operator with batch processing"""

    def __init__(self):
        self.tercen_ctx = ctx.TercenContext()
        self.tmp_dir = tempfile.gettempdir()
        self.mcr_path = "/opt/mcr/v99"
        self.matlab_exe = "/mcr/exe/run_pamsoft_grid_batch.sh"

    def get_doc_id_columns(self):
        """Extract documentId columns from context"""
        col_names = self.tercen_ctx.cnames
        doc_id_cols = [col for col in col_names if 'documentId' in col]

        if len(doc_id_cols) == 0 or len(doc_id_cols) > 2:
            raise ValueError("Either 1 or 2 documentId columns expected.")

        return doc_id_cols

    def prep_image_folder(self, doc_id_cols):
        """Download images from Tercen document storage"""

        # Send progress event
        self.send_progress(0, 1, "Downloading image files")

        # Load data from Tercen documents
        # Note: This requires tim library equivalent in Python
        # For now, we'll outline the structure

        # Get document IDs from context
        doc_ids = self.tercen_ctx.select(doc_id_cols)

        # Download files (placeholder - needs tim Python implementation)
        # In R: tim::load_data(ctx, unique(unlist(docIds[1])))
        # Python equivalent would need tercen file download API

        # For now, return structure similar to R version
        img_info = {
            'image_results_path': None,
            'file_ext': 'tif',
            'layout_dir': None
        }

        self.send_progress(1, 1, "Downloading image files")

        return img_info

    def get_operator_props(self, img_info):
        """Extract operator properties from Tercen context"""

        # Default properties
        props = {
            'sqcMinDiameter': 0.45,
            'sqcMaxDiameter': 0.85,
            'grdSpotPitch': 0,
            'grdSpotSize': 0.66,
            'grdRotation': list(range(-2, 2, 0.25)),
            'qntSaturationLimit': 4095,
            'segMethod': 'Edge',
            'segEdgeSensitivity': [0, 0.05]
        }

        # Get operator settings from context
        operator_props = self.tercen_ctx.query.operatorSettings.operatorRef.propertyValues

        for prop in operator_props:
            name = prop.get('name')
            value = prop.get('value')

            if name == "Min Diameter":
                props['sqcMinDiameter'] = float(value)
            elif name == "Max Diameter":
                props['sqcMaxDiameter'] = float(value)
            elif name == "Rotation":
                if value == "0":
                    props['grdRotation'] = [0]
                else:
                    # Parse "min:step:max" format
                    parts = [float(x) for x in value.split(':')]
                    props['grdRotation'] = list(range(parts[0], parts[2], parts[1]))
            elif name == "Saturation Limit":
                props['qntSaturationLimit'] = float(value)
            elif name == "Spot Pitch":
                props['grdSpotPitch'] = float(value)
            elif name == "Spot Size":
                props['grdSpotSize'] = float(value)
            elif name == "Edge Sensitivity":
                props['segEdgeSensitivity'][1] = float(value)

        # Auto-detect spot pitch if needed
        if props['grdSpotPitch'] == 0:
            img_type = self.detect_imageset_type(img_info['image_results_path'])
            if img_type == 'evolve3':
                props['grdSpotPitch'] = 17.0
            elif img_type == 'evolve2':
                props['grdSpotPitch'] = 21.5
            else:
                raise ValueError("Cannot automatically detect Spot Pitch")

        # Get array layout file
        layout_pattern = f"{img_info['layout_dir']}/*Layout*"
        layout_files = list(Path(img_info['layout_dir']).glob('*Layout*'))
        props['arraylayoutfile'] = str(layout_files[0]) if layout_files else ""

        return props

    def detect_imageset_type(self, img_path):
        """Detect Evolve2 vs Evolve3 based on image dimensions"""
        # This would use PIL/Pillow to read TIFF header
        # Placeholder for now
        return 'evolve2'

    def prep_batch_config(self, df, props, img_info, num_cores):
        """Create batch configuration JSON for MATLAB"""

        # Extract image column
        image_cols = [col for col in df.columns if 'Image' in col]
        if not image_cols:
            raise ValueError("No image column found")

        image_col = image_cols[0]

        # Group by .ci (cluster index)
        groups = df['.ci'].unique()
        image_groups = []

        for grp in groups:
            grp_df = df[df['.ci'] == grp]

            # Build image list for this group
            image_list = []
            for img_name in grp_df[image_col]:
                img_path = f"{img_info['image_results_path']}/{img_name}.{img_info['file_ext']}"
                image_list.append(img_path)

            # Create group configuration
            group_config = {
                'groupId': str(grp),
                'sqcMinDiameter': props['sqcMinDiameter'],
                'sqcMaxDiameter': props['sqcMaxDiameter'],
                'segEdgeSensitivity': props['segEdgeSensitivity'],
                'qntSeriesMode': 0,
                'qntShowPamGridViewer': 0,
                'grdSpotPitch': props['grdSpotPitch'],
                'grdSpotSize': props['grdSpotSize'],
                'grdRotation': props['grdRotation'],
                'qntSaturationLimit': props['qntSaturationLimit'],
                'segMethod': props['segMethod'],
                'grdUseImage': 'Last',
                'pgMode': 'grid',
                'dbgShowPresenter': 0,
                'arraylayoutfile': props['arraylayoutfile'],
                'imageslist': image_list
            }

            image_groups.append(group_config)

        # Create batch configuration
        batch_config = {
            'mode': 'batch',
            'numWorkers': num_cores,
            'progressFile': f"{self.tmp_dir}/progress.txt",
            'outputFile': f"{self.tmp_dir}/batch_results.csv",
            'imageGroups': image_groups
        }

        # Write JSON file
        json_file = f"{self.tmp_dir}/batch_config.json"
        with open(json_file, 'w') as f:
            json.dump(batch_config, f, indent=2)

        return json_file

    def run_matlab_batch(self, json_file, max_timeout_min=30):
        """Execute MATLAB batch processor using subprocess"""

        print(f"Launching MATLAB batch processor...")
        print(f"Config: {json_file}")

        # Build command
        cmd = [self.matlab_exe, self.mcr_path, f"--param-file={json_file}"]

        # Create log files
        out_log = tempfile.NamedTemporaryFile(mode='w', suffix='.log', delete=False)
        err_log = tempfile.NamedTemporaryFile(mode='w', suffix='.err', delete=False)

        try:
            # Start MATLAB process
            process = subprocess.Popen(
                cmd,
                stdout=out_log,
                stderr=err_log,
                cwd=self.tmp_dir
            )

            # Monitor progress
            progress_file = f"{self.tmp_dir}/progress.txt"
            last_progress = ""
            start_time = time.time()
            check_interval = 5  # seconds

            while process.poll() is None:  # While process is running
                time.sleep(check_interval)

                # Check timeout
                elapsed = time.time() - start_time
                if elapsed > max_timeout_min * 60:
                    process.kill()
                    raise TimeoutError(f"MATLAB process timed out after {max_timeout_min} minutes")

                # Read and update progress
                if os.path.exists(progress_file):
                    try:
                        with open(progress_file, 'r') as f:
                            progress = f.readline().strip()

                        if progress and progress != last_progress:
                            last_progress = progress
                            print(f"Progress: {progress}")

                            # Parse progress: "actual/total: message"
                            if ': ' in progress:
                                counts_str, message = progress.split(': ', 1)
                                actual, total = map(int, counts_str.split('/'))
                                self.send_progress(actual, total, message)
                    except Exception as e:
                        print(f"Warning: Could not read progress: {e}")

            # Get exit code
            exit_code = process.returncode

            out_log.close()
            err_log.close()

            # Handle errors
            if exit_code != 0:
                with open(out_log.name, 'r') as f:
                    out_content = f.read()
                with open(err_log.name, 'r') as f:
                    err_content = f.read()

                raise RuntimeError(
                    f"MATLAB batch processing failed with exit code {exit_code}\n"
                    f"Stderr: {err_content}\n"
                    f"Stdout: {out_content}"
                )

            print("MATLAB batch processing completed successfully")
            return 0

        finally:
            # Cleanup
            if os.path.exists(out_log.name):
                os.unlink(out_log.name)
            if os.path.exists(err_log.name):
                os.unlink(err_log.name)

    def parse_batch_results(self, output_file):
        """Parse aggregated results from MATLAB"""

        if not os.path.exists(output_file):
            raise FileNotFoundError(f"Output file not found: {output_file}")

        # Read CSV results
        results = pd.read_csv(output_file)

        if results.empty:
            raise ValueError("Output file is empty - no results generated")

        # Convert groupId back to .ci (integer)
        results['.ci'] = results['groupId'].astype(int)
        results = results.drop('groupId', axis=1)

        # Rename and convert columns to match Tercen expectations
        column_mapping = {
            'qntSpotID': 'ID',
            'grdIsReference': 'IsReference',
            'grdRow': 'spotRow',
            'grdCol': 'spotCol',
            'isManual': 'manual',
            'segIsBad': 'bad',
            'segIsEmpty': 'empty'
        }

        results = results.rename(columns=column_mapping)

        # Convert boolean columns
        results['IsReference'] = results['IsReference'].astype(bool).astype(str)

        # Ensure numeric types
        numeric_cols = ['spotRow', 'spotCol', 'grdXFixedPosition', 'grdYFixedPosition',
                       'gridX', 'gridY', 'diameter', 'manual', 'bad', 'empty', 'grdRotation']
        for col in numeric_cols:
            if col in results.columns:
                results[col] = pd.to_numeric(results[col], errors='coerce')

        # Sort by .ci
        results = results.sort_values('.ci').reset_index(drop=True)

        return results

    def send_progress(self, actual, total, message):
        """Send progress event to Tercen"""
        task = self.tercen_ctx.task
        if task:
            # Note: Tercen Python client progress event API
            # Placeholder - actual implementation depends on tercen client
            print(f"Progress: {actual}/{total} - {message}")

    def run(self):
        """Main execution flow"""

        print("Starting Pamsoft Grid Operator (Python)")

        # Validate input
        doc_id_cols = self.get_doc_id_columns()

        labels = self.tercen_ctx.labels
        if not labels:
            raise ValueError("Label factor containing the image name must be defined")

        # Download images
        img_info = self.prep_image_folder(doc_id_cols)

        # Get operator properties
        props = self.get_operator_props(img_info)

        # Get input data
        df = self.tercen_ctx.select(['.ci', labels[0]])

        # Determine resources
        n_groups = df['.ci'].nunique()
        n_cores = min(n_groups, 8)  # Cap at 8

        print(f"Processing {n_groups} image groups with {n_cores} workers")

        # Request resources
        self.tercen_ctx.request_resources(
            n_cpus=n_cores,
            ram=500000000,
            ram_per_cpu=500000000
        )

        # Send initial progress
        self.send_progress(0, n_groups, f"Preparing batch processing for {n_groups} groups")

        # Create batch configuration
        json_file = self.prep_batch_config(df, props, img_info, n_cores)

        # Execute MATLAB batch processor
        self.send_progress(0, n_groups, "Starting MATLAB batch processing")
        exit_code = self.run_matlab_batch(json_file, max_timeout_min=30)

        if exit_code != 0:
            raise RuntimeError(f"MATLAB batch processing failed with exit code: {exit_code}")

        # Parse results
        output_file = f"{self.tmp_dir}/batch_results.csv"
        results = self.parse_batch_results(output_file)

        # Upload results to Tercen
        self.send_progress(n_groups, n_groups, "Uploading results to Tercen")
        results = self.tercen_ctx.add_namespace(results)
        self.tercen_ctx.save(results)

        # Cleanup
        os.unlink(json_file)
        os.unlink(output_file)
        if os.path.exists(f"{self.tmp_dir}/progress.txt"):
            os.unlink(f"{self.tmp_dir}/progress.txt")

        print("Operator completed successfully")


if __name__ == "__main__":
    try:
        operator = PamsoftGridOperator()
        operator.run()
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
